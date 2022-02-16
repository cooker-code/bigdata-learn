短窗口的计算由于其窗口期较短，那么很快就能获取到结果，但是对于长窗口来说窗口时间比较长，如果等窗口期结束才能看到结果，那么这份数据就不具备实时性，大多数情况我们希望能够看到一个长窗口的结果不断变动的情况，对此Flink提供了ContinuousEventTimeTrigger连续事件时间触发器与ContinuousProcessingTimeTrigger连续处理时间触发器，指定一个固定时间间隔interval，不需要等到窗口结束才能获取结果，能够在固定的interval获取到窗口的中间结果。

ContinuousEventTimeTrigger
ContinuousEventTimeTrigger表示连续事件时间触发器，用在EventTime属性的任务流中，以事件时间的进度来推动定期触发

public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {

       --part 1

        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {

            // if the watermark is already past the window fire immediately

            return TriggerResult.FIRE;

        } else {

            ctx.registerEventTimeTimer(window.maxTimestamp());

        }

       ---part 2

        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

        if (fireTimestamp.get() == null) {

            long start = timestamp - (timestamp % interval);

            long nextFireTimestamp = start + interval;

            ctx.registerEventTimeTimer(nextFireTimestamp);

            fireTimestamp.add(nextFireTimestamp);

        }

        return TriggerResult.CONTINUE;

    }

对于每一条数据都会经过onElement 处理，

part1部分，用于判断是否触发窗口函数或者注册一个窗口endTime的定时触发器, endTime定时器最终触发窗口函数，能够得到一个最终的窗口结果

part2部分， ReducingState用于存储下一次的触发时间，初始值是null, 会根据第一个数据时间(dataTime)、来判断第一次触发的时间(dataTime+interval)，并且注册一个触发定时器，当watermark大于nextFireTimestamp就是执行其onEventTime方法，

public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {

       //窗口结束的触发

        if (time == window.maxTimestamp()){

            return TriggerResult.FIRE;

        }

        ReducingState<Long> fireTimestampState = ctx.getPartitionedState(stateDesc);

        Long fireTimestamp = fireTimestampState.get();

        if (fireTimestamp != null && fireTimestamp == time) {

            fireTimestampState.clear();

            fireTimestampState.add(time + interval);

            ctx.registerEventTimeTimer(time + interval);

            return TriggerResult.FIRE;

        }

        return TriggerResult.CONTINUE;

    }

在onEventTime 中会获取当前的触发时间fireTimestamp，然后注册下一个fireTimestamp+interval的触发器。可以看到反复的定时注册会导致其不断的循序下去，当窗口期结束肯定是需要结束该窗口的持续触发调用，那么是如何做到的呢？在WindowOperator中onEventTime触发定时调用中会判断如果是窗口结束时间的触发调用会执行clearAllState方法，在该方法中会调用triggerContext.clear()，也就是会调用ContinuousEventTimeTrigger的clear方法，

    public void clear(W window, TriggerContext ctx) throws Exception {

        ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);

        Long timestamp = fireTimestamp.get();

        if (timestamp != null) {

            ctx.deleteEventTimeTimer(timestamp);

            fireTimestamp.clear();

        }

    }

那么其会删除下一次该窗口器的触发并且清除对应的ReducingState 状态数据。

在使用ContinuousEventTimeTrigger 有以下点需要注意

连续定时触发与第一条数据有关，例如第一条数据是2019-11-16 11:22:01， 10s触发一次，那么后续触发时间就分别是2019-11-16 11:22:10、2019-11-16 11:22:20、2019-11-16 11:22:30

如果数据时间间隔相对于定期触发的interval比较大，那么有可能会存在多次输出相同结果的场景，比喻说触发的interval是10s, 第一条数据时间是2019-11-16 11:22:00, 那么下一次的触发时间是2019-11-16 11:22:10， 如果此时来了一条2019-11-16 11:23:00 的数据，会导致其watermark直接提升了1min, 会直接触发5次连续输出，对于下游处理来说可能会需要做额外的操作。

窗口的每一个key的触发时间可能会不一致，是因为窗口的每一个key对应的第一条数据时间不一样，正如上述所描述定时规则。由于会注册一个窗口endTime的触发器，会触发窗口所有key的窗口函数，保证最终结果的正确性。
ContinuousProcessingTimeTrigger表示处理时间连续触发器，其思想与ContinuousEventTimeTrigger触发器大体相同，主要区别就是基于处理时间的定时触发。

使用案例
场景：求每个区域的每小时的商品销售额, 要求每隔1min能能够看到销售额变动情况。代码实现如下：

case class AreaOrder(areaId: String, amount: Double)

case class Order(orderId: String, orderTime: Long, gdsId: String, amount: Double, areaId: String)



object ContinuousEventTimeTriggerDemo {



def main(args: Array[String]): Unit = {



    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.getConfig.setAutoWatermarkInterval(5000L)

    env.setParallelism(1)



    val kafkaConfig = new Properties();

    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");



    val consumer = new FlinkKafkaConsumer011[String]("topic1", new SimpleStringSchema(), kafkaConfig)

    env.addSource(consumer)

      .map(x => {

        val a = x.split(",")

        Order(a(0), a(1).toLong, a(2), a(3).toDouble, a(4))

      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(30)) {

      override def extractTimestamp(element: Order): Long = element.orderTime

    }).map(x => {

      AreaOrder(x.areaId, x.amount)

    })

      .keyBy(_.areaId)

      .timeWindow(Time.hours(1))

      .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))

      .reduce(new ReduceFunction[AreaOrder] {

        override def reduce(value1: AreaOrder, value2: AreaOrder): AreaOrder = {

          AreaOrder(value1.areaId, value1.amount + value2.amount)

        }

      }).print()

    env.execute()

}

}

测试数据：

orderId03,1573874530000,gdsId03,300,beijing  (2019-11-16 11:22:10,下一个触发时间是2019-11-16 11:23:00)

orderId03,1573874740000,gdsId03,300,hangzhou  (2019-11-16 11:25:40,下一个触发时间是2019-11-16 11:26:00)

当第二条数据发送出去之后，可以得到输出结果：

AreaOrder(beijing,300.0)

AreaOrder(beijing,300.0)

AreaOrder(beijing,300.0)

被连续触发三次，由于此时的watermark是2019-11-16 11:25:35，会触发beijing区域的注册定时时间2019-11-16 11:23:00的定时器，接着又会注册下一个1min的定时触发器，直到下一个触发时间为2019-11-16 11:26:00停止触发。