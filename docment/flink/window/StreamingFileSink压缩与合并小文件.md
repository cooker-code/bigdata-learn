Flink目前对于外部Exactly-Once写支持提供了两种的sink，一个是Kafka-Sink，另一个是Hdfs-Sink，这两种sink实现的Exactly-Once都是基于Flink checkpoint提供的hook来实现的两阶段提交模式来保证的，主要应用在实时数仓、topic拆分、基于小时分析处理等场景下。本篇将会介绍StreamingFileSink的基本用法、如何压缩数据以及合并产生的小文件。

一、基本用法
StreamingFileSink提供了基于行、列两种文件写入格式，用法：

//行
StreamingFileSink.forRowFormat(new Path(path),
new SimpleStringEncoder<T>())
.withBucketAssigner(new PaulAssigner<>()) //分桶策略
.withRollingPolicy(new PaulRollingPolicy<>()) //滚动策略
.withBucketCheckInterval(CHECK_INTERVAL) //检查周期
.build();

//列 parquet
StreamingFileSink.forBulkFormat(new Path(path),
ParquetAvroWriters.forReflectRecord(clazz))
.withBucketAssigner(new PaulBucketAssigner<>())
.withBucketCheckInterval(CHECK_INTERVAL)
.build();
这两种写入格式除了文件格式的不同，另外一个很重要的区别就是回滚策略的不同，forRowFormat行写可基于文件大小、滚动时间、不活跃时间进行滚动，但是对于forBulkFormat列写方式只能基于checkpoint机制进行文件滚动，即在执行snapshotState方法时滚动文件，如果基于大小或者时间滚动文件，那么在任务失败恢复时就必须对处于in-processing状态的文件按照指定的offset进行truncate，我想这是由于列式存储是无法针对文件offset进行truncate的，因此就必须在每次checkpoint使文件滚动，其使用的滚动策略实现是OnCheckpointRollingPolicy。
二、文件压缩
通常情况下生成的文件用来做按照小时或者天进行分析，但是离线集群与实时集群是两个不同的集群，那么就需要将数据写入到离线集群中，在这个过程中数据流量传输成本会比较高，因此可以选择parquet文件格式，然而parquet存储格式默认是不压缩格式：
//ParquetWriter.Builder中
private CompressionCodecName codecName = DEFAULT_COMPRESSION_CODEC_NAME;
在Flink中的ParquetAvroWriters未提供压缩格式的入口，但是可以自定义一个ParquetAvroWriters，在创建ParquetWriter时，指定压缩算法：
public class PaulParquetAvroWriters {

    public static <T extends SpecificRecordBase> ParquetWriterFactory<T> forSpecificRecord(Class<T> type,CompressionCodecName compressionCodecName) {
        final String schemaString = SpecificData.get().getSchema(type).toString();
        final ParquetBuilder<T> builder = (out) -> createAvroParquetWriter(schemaString, SpecificData.get(), out,compressionCodecName);
        return new ParquetWriterFactory<>(builder);
    }
//compressionCodecName 压缩算法
public static ParquetWriterFactory<GenericRecord> forGenericRecord(Schema schema,CompressionCodecName compressionCodecName) {
final String schemaString = schema.toString();
final ParquetBuilder<GenericRecord> builder = (out) -> createAvroParquetWriter(schemaString, GenericData.get(), out,compressionCodecName);
return new ParquetWriterFactory<>(builder);
}
//compressionCodecName 压缩算法
public static <T> ParquetWriterFactory<T> forReflectRecord(Class<T> type,CompressionCodecName compressionCodecName) {
final String schemaString = ReflectData.get().getSchema(type).toString();
final ParquetBuilder<T> builder = (out) -> createAvroParquetWriter(schemaString, ReflectData.get(), out,compressionCodecName);
return new ParquetWriterFactory<>(builder);
}
//compressionCodecName 压缩算法
private static <T> ParquetWriter<T> createAvroParquetWriter(
String schemaString,
GenericData dataModel,
OutputFile out,
CompressionCodecName compressionCodecName) throws IOException {
final Schema schema = new Schema.Parser().parse(schemaString);
return AvroParquetWriter.<T>builder(out)
.withSchema(schema)
.withDataModel(dataModel)
.withCompressionCodec(compressionCodecName)//压缩算法
.build();
}
private PaulParquetAvroWriters() {}
}
那么在使用时根据实际情况传入SNAPPY、LZO、GZIP等压缩算法，但是需要注意的压缩虽然减少了io的消耗，带来的却是cpu的更多消耗，在实际使用中进行权衡。
三、小文件处理
不管是Flink还是SparkStreaming写hdfs不可避免需要关注的一个点就是如何处理小文件，众多的小文件会带来两个影响：

Hdfs NameNode维护元数据成本增加
下游hive/spark任务执行的数据读取成本增加
理想状态下是按照设置的文件大小滚动，那为什么会产生小文件呢？这与文件滚动周期、checkpoint时间间隔设置相关，如果滚动周期较短、checkpoint时间也比较短或者数据流量有低峰期达到文件不活跃的时间间隔，很容易产生小文件，接下来介绍几种处理小文件的方式：

减少并行度
回顾一下文件生成格式：part + subtaskIndex + connter，其中subtaskIndex代表着任务并行度的序号，也就是代表着当前的一个写task，越大的并行度代表着越多的subtaskIndex，数据就越分散，如果我们减小并行度，数据写入由更少的task来执行，写入就相对集中，这个在一定程度上减少的文件的个数，但是在减少并行的同时意味着任务的并发能力下降；

增大checkpoint周期或者文件滚动周期
以parquet写分析为例，parquet写文件由processing状态变为pending状态发生在checkpoint的snapshotState阶段中，如果checkpoint周期时间较短，就会更快发生文件滚动，增大checkpoint周期，那么文件就能积累更多数据之后发生滚动，但是这种增加时间的方式带来的是数据的一定延时；

下游任务合并处理
待Flink将数据写入hdfs后，下游开启一个hive或者spark定时任务，通过改变分区的方式，将文件写入新的目录中，后续任务处理读取这个新的目录数据即可，同时还需要定时清理产生的小文件，这种方式虽然增加了后续的任务处理成本，但是其即合并了小文件提升了后续任务分析速度，也将小文件清理了减小了对NameNode的压力，相对于上面两种方式更加稳定，因此也比较推荐这种方式。
四、总结
本文重点分析了StreamingFileSink用法、压缩与小文件合并方式，StreamingFileSink支持行、列两种文件写入格式，对于压缩只需要自定义一个ParquetAvroWriters类，重写其createAvroParquetWriter方法即可，对于小文件合并比较推荐使用下游任务合并处理方式。