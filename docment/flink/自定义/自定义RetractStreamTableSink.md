
### Flink Table/SQL 中对于流表TableSink的定义有三类：AppendStreamTable、RetractStreamTableSink 、UpsertStreamTableSink 
这三类主要区别对应不同的流类型，在我看来可以归纳为两种模式：
> Insert模式，对应AppendStreamTable，只能执行insert动作，例如窗口聚合结果，每个窗口的结果都是唯一的，不会影响之前窗口的输出结果

> Redo模式，对应RetractStreamTableSink /UpsertStreamTableSink，除了执行Insert动作，还可执行Update/Delete动作，也就是结果可更新，例如全局group by聚合，后面的结果会影响之前的输出，
>RetractStreamTableSink与UpsertStreamTableSink的区别主要在于消息编码格式不同，如果产生一条结果数据需要更新，RetractStreamTableSink需要编码两条消息Delete与Insert,而UpsertStreamTableSink只需要编码成为一条upsert消息即可
  使用Insert模式同样可以使用Redo模式，但是会多了编码步骤,但是其更加具有通用型，一般会实现Redo的TableSink,接下来两篇主要介绍如何实现自定义RetractStreamTableSink 与 UpsertStreamTableSink，首先看RetractStreamTableSink的自定义实现：



class PaulRetractStreamTableSink extends RetractStreamTableSink[Row] {



private var fieldNames:Array[String]=_

private var fieldTypes:Array[TypeInformation[_]]=_



/**

    * 该tableSink的schema

    * @return

    */

override def getRecordType: TypeInformation[Row] = {

     new RowTypeInfo(fieldTypes,fieldNames)

}



/**

    * @param dataStream 上游的table 在内部会换为dataStream

    * Boolean: 表示消息编码，insert 会编码为true,只会产生一条数据

    *                     update 会编码为false/true, 会产生两条数据，

    *                            第一条false表示需要撤回，也就是删除的数据

    *                            第二条true表示需要插入的数据

    * Row : 表示真正的数据

    */

override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {

    dataStream.print()

}



/**

    * 字段名称

    * @return

    */

override def getFieldNames: Array[String] = fieldNames



/**

    * 字段类型

    * @return

    */

override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes



/**

    * 内部调用，会自动将上游输出类型、字段配置进来

    * @param fieldNames

    * @param fieldTypes

    * @return

    */

override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {

    this.fieldNames=fieldNames

    this.fieldTypes=fieldTypes

    this

}

}

以全局wordcount 为例



object Demo1 {



def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tabEnv = TableEnvironment.getTableEnvironment(env)

    val kafkaConfig = new Properties()

    kafkaConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    kafkaConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "gid1")

    val consumer = new FlinkKafkaConsumer011[String]("topic1",

      new SimpleStringSchema, kafkaConfig)

    val ds = env.addSource(consumer)

      .map((_, 1))

    tabEnv.registerDataStream("table1", ds, 'word, 'cnt)

    val rsTable = tabEnv.sqlQuery("select word,sum(cnt) from table1 group by word")

    val tableSink = new PaulRetractStreamTableSink

    rsTable.writeToSink(tableSink)

    env.execute()

}

}

当kafka端生产一条数据:a, 控制台显示：

2> (true,a,1)

接着在产生一条数据：a, 控制台显示：

2> (true,a,1)

2> (false,a,1)

2> (true,a,2)

此时产生两条数据：false表示需要撤回的，true表示需要插入的。



在实际应用中通常会定义一个SinkFunction, 将emitDataStream中dataStream输出，另外会将word 字段在外部存储设置为key，那我们就可以忽略编码为false 的消息，每次只需要根据key覆盖外部结果即可。