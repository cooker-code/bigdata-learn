在上一篇的分析【[Flink DataStream中CoGroup实现原理与三种 join 实现](https://mp.weixin.qq.com/s?__biz=MzU5MTc1NDUyOA==&mid=2247483962&idx=1&sn=cd6a1c3687188088e61bc309529295b8&chksm=fe2b6675c95cef63731588cb572dd7bcb921d5db7a4ecc8692d8466f98d0c35e78148284802a&token=1137454042&lang=zh_CN&scene=21#wechat_redirect)】中基于DataStream的join只能实现在同一个窗口的两个数据流之间进行join, 但是在实际中常常是会存在数据乱序或者延时的情况，导致两个流的数据进度不一致，就会出现数据跨窗口的情况，那么数据就无法在同一个窗口内join。flink 基于KeyedStream提供了一种interval join 机制，intervaljoin 连接两个keyedStream, 按照相同的key在一个相对数据时间的时间段内进行连接。

先看一个假设的案例：用户购买商品过程中填写收货地址然后下单，在这个过程中产生两个数据流，一个是订单数据流包含用户id、商品id、订单时间、订单金额、收货id等，另一个是收货信息数据流包含收货id、收货人、收货人联系方式、收货人地址等，系统在处理过程中，先发送订单数据，在之后的1到5秒内会发送收货数据，现在要求实时统计按照不同区域维度的订单金额的top100地区。在这个案例中两个数据流：订单流orderStream先，收货信息流addressStream后，需要将这两个数据流按照收货id join之后计算top100订单金额的地区，由于orderStream比addressStream早1到5秒，那么就有这样一个关系：
`orderStream.time+1<=addressStream.time<=orderStream.time+5` 或者是
`addressStream.time-5<=orderStream.time<=addressStream.time-1`
看下join 部分代码实现：

```
case class Order(orderId:String, userId:String, gdsId:String, amount:Double, addrId:String, time:Long)case class Address(addrId:String, userId:String, address:String, time:Long)case class RsInfo(orderId:String, userId:String, gdsId:String, amount:Double, addrId:String, address:String)objectIntervalJoinDemo{def main(args:Array[String]):Unit={    val env =StreamExecutionEnvironment.getExecutionEnvironment    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)    env.getConfig.setAutoWatermarkInterval(5000L)    env.setParallelism(1)
    val kafkaConfig =newProperties()    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1")
    val orderConsumer =newFlinkKafkaConsumer011[String]("topic1",newSimpleStringSchema, kafkaConfig)    val addressConsumer =newFlinkKafkaConsumer011[String]("topic2",newSimpleStringSchema, kafkaConfig)
    val orderStream = env.addSource(orderConsumer).map(x =>{        val a = x.split(",")newOrder(a(0), a(1), a(2), a(3).toDouble, a(4), a(5).toLong)}).assignTimestampsAndWatermarks(newBoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(10)){overridedef extractTimestamp(element:Order):Long= element.time}).keyBy(_.addrId)
    val addressStream = env.addSource(addressConsumer).map(x =>{        val a = x.split(",")newAddress(a(0), a(1), a(2), a(3).toLong)}).assignTimestampsAndWatermarks(newBoundedOutOfOrdernessTimestampExtractor[Address](Time.seconds(10)){overridedef extractTimestamp(element:Address):Long= element.time}).keyBy(_.addrId)
    orderStream.intervalJoin(addressStream).between(Time.seconds(1),Time.seconds(5)).process(newProcessJoinFunction[Order,Address,RsInfo]{overridedef processElement(left:Order, right:Address, ctx:ProcessJoinFunction[Order,Address,RsInfo]#Context,out:Collector[RsInfo]):Unit={          println("==在这里得到相同key的两条数据===")          println("left:"+ left)          println("right:"+ right)}})    env.execute()}}
```

topic1生产数据：
`order01,userId01,gds01,100,addrId01,1573054200000`
topic2生产数据：
`addrId01,userId01,beijing,1573054203000`
由于满足时间范围的条件，得到结果：
`left:Order(order01,userId01,gds01,100.0,addrId01,1573054200000)`
`right:Address(addrId01,userId01,beijing,1573054203000)`
但是如果topic2接着在生产数据：
`addrId01,userId01,beijing,1573054206000`
此时addressStream.time+5>orderStream.time ，没有结果输出。

从源码角度理解intervaljoin实现：

1. intervaljoin首先会将两个KeyedStream 进行connect操作得到一个ConnectedStreams， ConnectedStreams表示的是连接两个数据流，并且这两个数据流之前可以实现状态共享, 对于intervaljoin 来说就是两个流相同key的数据可以相互访问
2. 在ConnectedStreams之上进行IntervalJoinOperator算子操作，该算子是intervaljoin 的核心，接下来分析一下其实现
   a. 定义了两个`MapState<Long, List<BufferEntry<T1>>>`类型的状态对象，分别用来存储两个流的数据，其中Long对应数据的时间戳，`List<BufferEntry<T1>>`对应相同时间戳的数据
   b. 包含processElement1、processElement2两个方法，这两个方法都会调用processElement方法，真正数据处理的地方

![图片](640-20220216102355351)

   

- 判断延时，数据时间小于当前的watermark值认为数据延时，则不处理
- 将数据添加到对应的`MapState<Long, List<BufferEntry<T1>>>`缓存状态中，key为数据的时间
- 循环遍历另外一个状态,如果满足`ourTimestamp + relativeLowerBound <=timestamp<= ourTimestamp + relativeUpperBound` , 则将数据输出给ProcessJoinFunction调用，ourTimestamp表示流入的数据时间，timestamp表示对应join的数据时间
- 注册一个数据清理时间方法，会调用onEventTime方法清理对应状态数据。对于例子中orderStream比addressStream早到1到5秒，那么orderStream的数据清理时间就是5秒之后，也就是orderStream.time+5,当watermark大于该时间就需要清理，对于addressStream是晚来的数据不需要等待，当watermark大于数据时间就可以清理掉。

整个处理逻辑都是基于数据时间的，也就是**intervaljoin 必须基于EventTime语义**，在between 中有做TimeCharacteristic是否为EventTime校验, 如果不是则抛出异常。