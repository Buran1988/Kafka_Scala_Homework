package consumer_logic

import java.io.IOException
import java.time.Duration
import java.util
import java.util.{Date, Properties}
import argument_support.ApplicationArguments
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import models.{DeviceMeasurements, Key}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileAlreadyExistsException, Path}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class KConsumer(APP_ARGS: ApplicationArguments) {

  private val consumerProps = new Properties() {
    put(ConsumerConfig.CLIENT_ID_CONFIG, "Homework demo client")
    put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, APP_ARGS.brokers)
    put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)
    put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[KafkaAvroDeserializer].getCanonicalName)
    put(ConsumerConfig.GROUP_ID_CONFIG, "example-consumer-group")
    put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, APP_ARGS.schema)
  }
  private lazy val consumer = new KafkaConsumer[Key, DeviceMeasurements](consumerProps)
  private lazy val SCHEMA = AvroSchema[DeviceMeasurements]
  private lazy val TOPIC = APP_ARGS.topic
  private val MAX_DURATION: java.time.Duration = Duration.ofHours(2)

  def consumeRecords(): Unit = {
    //subscribe to producer's topic
    consumer.subscribe(util.Arrays.asList(TOPIC))


    var endOffset: java.lang.Long = null

    val timeFrame = APP_ARGS.timeframe * 1000
    var startTime = new Date().getTime()

    consumer.poll(MAX_DURATION)

    while (true) {

      if (new Date().getTime >= startTime + timeFrame) {

        val topicPartitionLongMap = seekOffsets(startTime)
        startTime = new Date().getTime()

        for (entry <- topicPartitionLongMap.entrySet) {
          //          var records = consumer.poll(100)
          consumer.seek(entry.getKey, entry.getValue)
          val records = consumer.poll(MAX_DURATION)

          val generic_data_array = new ArrayBuffer[GenericData.Record](records.count())

          for (record <- records) {
            val formatValue = RecordFormat[DeviceMeasurements]
            val thisRecord = record.value.asInstanceOf[GenericRecord]
            val measurement = formatValue.from(thisRecord)

            generic_data_array += mapGenericDataFields(measurement)

            println("Timestamp: " + record.timestamp + ", offset: " + record.offset + ", amount: " + measurement.valueType)
            endOffset = record.offset()
          }

          if (generic_data_array.nonEmpty) {
            try writeToParquet(generic_data_array, new Path(APP_ARGS.path_to_parquet + "/default_" + endOffset + ".parquet"))
            catch {
              case file_exists: FileAlreadyExistsException => println(file_exists.getMessage)
              case unknownError: UnknownError => println(unknownError.getMessage)
            }
          }
          consumer.commitAsync()
        }
      }
    }

  }

  private def mapGenericDataFields(m: DeviceMeasurements): GenericData.Record = {

    val generic_data: GenericData.Record = new GenericData.Record(SCHEMA) {
      put("date", m.date)
      put("time", m.time)
      put("key", m.key)
      put("value", m.value)
      put("valueType", m.valueType)
    }
    generic_data
  }

  /**
    * Seek consumer to specific timestamp
    *
    * @param timestamp Unix timestamp in milliseconds to seek to.
    */
  private def seekOffsets(timestamp: java.lang.Long): util.Map[TopicPartition, java.lang.Long] = {

    val partitionInfos = consumer.partitionsFor(TOPIC)
    // Find offsets for timestamp
    val timestampMap = new util.HashMap[TopicPartition, java.lang.Long]

    for (partitionInfo <- partitionInfos) {
      timestampMap.put(new TopicPartition(TOPIC, partitionInfo.partition), timestamp)
    }

    val offsetMap = consumer.offsetsForTimes(timestampMap)

    // Build map of partition => offset
    val partitionOffsetMap = new util.HashMap[TopicPartition, java.lang.Long]

    for (entry <- offsetMap.entrySet) {
      if (entry.getValue != null) partitionOffsetMap.put(entry.getKey, entry.getValue.offset)
    }

    partitionOffsetMap
  }


  @throws[IOException]
  private def writeToParquet(recordsToWrite: ArrayBuffer[GenericData.Record], fileToWrite: Path = new Path("default.parquet")): Unit = {

    val writer = AvroParquetWriter
      .builder[GenericData.Record](fileToWrite)
      .withSchema(SCHEMA)
      .withConf(new Configuration)
      .withCompressionCodec(CompressionCodecName.SNAPPY).build

    try {
      for (record <- recordsToWrite) {
        writer.write(record)
      }
    }

    finally if (writer != null) writer.close()
  }

}
