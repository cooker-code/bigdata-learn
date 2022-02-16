本篇幅介绍Flink Table/SQL中如何自定义一个表函数(TableFunction)，介绍其基本用法以及与源码结合分析其调用流程。

基本使用
表函数TableFunction相对标量函数ScalarFunction一对一，它是一个一对多的情况，通常使用TableFunction来完成列转行的一个操作。先通过一个实际案例了解其用法：终端设备上报数据，数据类型包含温度、耗电量等，上报方式是以多条方式上报，例如：

图片

现在希望得到如下数据格式：

图片

这是一个典型的列转行或者一行转多行的场景，需要将data列进行拆分成为多行多列，先看下代码实现：

public class MyUDTF extends TableFunction<Row>{



public void eval(String s){

JSONArray jsonArray =JSONArray.parseArray(s);

for(int i =0; i < jsonArray.size(); i++){

JSONObject jsonObject = jsonArray.getJSONObject(i);

String type = jsonObject.getString("type");

String value = jsonObject.getString("value");

            collector.collect(Row.of(type, value));

}

}



@Overridepublic TypeInformation<Row> getResultType(){

returnTypes.ROW(Types.STRING(),Types.STRING());

}

}

在MyUDTF中继承了TableFunction<T>， 所有的自定义表函数都必须继承该抽象类，其中T表示返回的数据类型，通常如果是原子类型则直接指定例如String, 如果是复合类型通常会选择Row, FlinkSQL 通过类型提取可以自动识别返回的类型，如果识别不了需要重载其getResultType方法，指定其返回的TypeInformation，重点看下eval 方法定义：

eval 方法， 处理数据的方法，必须声明为public/not static，并且该方法可以重载，会自动根据不同的输入参数选择对应的eval, 在eval方法里面可以使用collector对象将数据发送出去，该对象是从TableFunction继承过来的。

调用如下：

def main(args:Array[String]):Unit={

    val env =StreamExecutionEnvironment.getExecutionEnvironment

    val tabEnv =TableEnvironment.getTableEnvironment(env)

    tabEnv.registerFunction("udtf",newMyUDTF)

    val kafkaConfig =newProperties();

    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");

    val consumer =newFlinkKafkaConsumer[String]("topic1",newSimpleStringSchema(), kafkaConfig);



    val ds:DataStream[(String, java.lang.Long,String)]= env.addSource(consumer)

.map(x =>{

        val obj = JSON.parseObject(x, classOf[RawData])

Tuple3.apply(obj.devId, obj.time, obj.data)

})



    tabEnv.registerDataStream("tbl1", ds,'devId, 'time,'data)

    val rsTab = tabEnv.sqlQuery("select devId,`time`,`type`,`value` from tbl1 , LATERAL TABLE(udtf(data)) as t(`type`,`value`) ")

      .writeToSink(new PaulRetractStreamTableSink)

    env.execute()

}

测试数据：

{"devid":"dev01","time":1574944573000,"data":[{"type":"temperature","value":"10"},{"type":"battery","value":"1"}]}

得到结果：

3>(true,dev01,1574944573000,temperature,10)

3>(true,dev01,1574944573000,battery,1)

至此拿到了符合要求的数据。在Flink SQL中使用TableFunction需要搭配LATERAL TABLE一起使用，将其认为是一张虚拟的表，整个过程就是一个Join with Table Function过程，左表(tbl1) 会join 右表(t1) 的每一条记录。但是也存在另外一种情况右表(t1)没有输出但是也需要左表输出那么可以使用LEFT JOIN LATERAL TABLE，用法如下：

SELECT users, tag

FROM Orders LEFT JOIN LATERAL TABLE(unnest_udtf(tags)) t AS tag ON TRUE

对于右表没有输出会自动补上null。

源码分析
在介绍源码分析之前先安利一个小技巧，很多时候比较难找到Flink SQL解析之后的任务具体执行过程，这个时候可以通过先打印其执行计划，使用方式：

println(tabEnv.explain(rsTab))

就可以得到其抽象语法树、逻辑执行计划、物理执行计划：

==AbstractSyntaxTree==

LogicalProject(devId=[$0], time=[$1], type=[$3], value=[$4])

LogicalCorrelate(correlation=[$cor0], joinType=[inner], requiredColumns=[{2}])

LogicalTableScan(table=[[tbl1]])

LogicalTableFunctionScan(invocation=[udtf($cor0.data)], rowType=[RecordType(VARCHAR(65536) f0, VARCHAR(65536) f1)], elementType=[class[Ljava.lang.Object;])



==OptimizedLogicalPlan==

DataStreamCalc(select=[devId, time, f0 AS type, f1 AS value])

DataStreamCorrelate(invocation=[udtf($cor0.data)], correlate=[table(udtf($cor0.data))],select=[devId, time, data, f0, f1], rowType=[RecordType(VARCHAR(65536) devId, BIGINT time, VARCHAR(65536) data, VARCHAR(65536) f0, VARCHAR(65536) f1)], joinType=[INNER])

DataStreamScan(table=[[tbl1]])



==PhysicalExecutionPlan==

Stage1:DataSource

    content : collect elements withCollectionInputFormat



Stage2:Operator

        content :Map

        ship_strategy : FORWARD



Stage3:Operator

            content :from:(devId, time, data)

            ship_strategy : FORWARD

...........

可以从逻辑执行计划入手，Table Function Join 对应DataStreamCorrelate，重点在于其translateToPlan方法：

generateFunction 调用，生成一个ProcessFunction函数，内部封装用户自定义的TableFunction, 在该ProcessFunction里面会调用TableFunction的eval方法，由于该Function是动态生成的，可以通过debug方法查看，这里感受一下在processElement里面调用eval的代码：

function_udf$MyUDTF$086f769e79e46e52752c8500480e4b32.eval(isNull$21 ?null:(java.lang.String) result$20);

generateCollector调用，生成的是一个TableFunctionCollector 类型的collector，这部分也是动态生成的

CRowCorrelateProcessRunner 也是一个ProcessFunction, 内部包含了generateFunction生成的function 与generateCollector生成的collector, 在其初始化open的时候会将该collector赋给function

接下来从CRowCorrelateProcessRunner的processElement方法看整个调用流程：

    cRowWrapper.out=out

    cRowWrapper.setChange(in.change)

    collector.setCollector(cRowWrapper)

    collector.setInput(in.row)//重点input 信息设置到动态生成的collector

    collector.reset()

function.processElement(

in.row,

      ctx.asInstanceOf[ProcessFunction[Row,Row]#Context],

      cRowWrapper)

这步调用动态生成的function, 在其processElement里面调用eval方法，eval 会调用动态生成的collector，这个步骤就可以理解为是一个join过程， 最终输出组合数据。