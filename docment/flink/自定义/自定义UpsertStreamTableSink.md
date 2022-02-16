在Flink实战系列之自定义RetractStreamTableSink中介绍了如何编写自定义RetractStreamTableSink，Flink 中提供了另外一种可Redo模式的UpsertStreamTableSink，与RetractStreamTableSink不同的是：

在UpsertStreamTableSink中需要指定一个unique key , 该unique key既可以是single的也可以是composite的 ，所有的消息编码都是针对该unique key的，不需要用户自已指定，会在任务解析过程中自动生成, 通常是group by 中字段

RetractStreamTableSink针对需要update消息生成delete 与insert两条消息，但是UpsertStreamTableSink只会生成一条消息，称之为upsert,即可表示插入也可表示更新。

仍然以全局wordCount 为例：



class PaulUpsertStreamTableSink extends UpsertStreamTableSink[Row] {



private var fieldNames:Array[String]=_

private var fieldTypes:Array[TypeInformation[_]]=_



private var keys: Array[String]=_

private var isAppendOnly:lang.Boolean=_



/**

    * unique key

    * @param keys

    */

override def setKeyFields(keys: Array[String]): Unit = {

         this.keys=keys

}



override def setIsAppendOnly(isAppendOnly: lang.Boolean): Unit = {

       this.isAppendOnly=isAppendOnly

}



override def getRecordType: TypeInformation[Row] = {

       new RowTypeInfo(fieldTypes,fieldNames)

}



override def emitDataStream(dataStream: DataStream[tuple.Tuple2[lang.Boolean, Row]]): Unit = {

    dataStream.print()

}



override def getFieldNames: Array[String] = fieldNames



override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes



override def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation[_]]): TableSink[tuple.Tuple2[lang.Boolean, Row]] = {



    this.fieldNames=fieldNames

    this.fieldTypes=fieldTypes

    this



}

}



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



    val tableSink = new PaulUpsertStreamTableSink

    rsTable.writeToSink(tableSink)

    env.execute()

}

}

当kafka端生产一条数据:a, 控制台显示：

2> (true,a,1)

接着在产生一条数据：a, 控制台显示

2> (true,a,1)

2> (true,a,2)

结果是并没有产生(false,a,1) 这条数据，因此比UpsertStreamTableSink更加有效率。

在这个例子中unique key 就表示word 字段，会调用setKeyFields自动设置。setIsAppendOnly 表示是否AppendOnly模式，在这个例子中包含更新模式，所以为false, 对于仅仅是单条插入或者窗口函数聚合类的表示的是AppendOnly，为true。需要注意的是unique key的存在与AppendOnly为true并没有必然关系，窗口函数聚合类的AppendOnly为true,同时存在unique key，单条输出(例如select word from table1)类的AppendOnly为true，但是unique key不存在。相反如果AppendOnly为false ，那么unique key则必然存在。