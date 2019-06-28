package producer_logic

import java.util.Properties
import java.util.concurrent.Future
import argument_support.ApplicationArguments
import com.sksamuel.avro4s.{FromRecord, RecordFormat, ToRecord}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

class KProducer[K <: Product, V <: Product](APP_ARGS: ApplicationArguments) {

  val kafkaProps = new Properties() {
    put(ProducerConfig.CLIENT_ID_CONFIG, "Homework demo client")
    put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, APP_ARGS.brokers)
    put(ProducerConfig.ACKS_CONFIG, "all")
    put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
    put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[KafkaAvroSerializer].getCanonicalName)
    put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, APP_ARGS.schema)
  }
  private lazy val producer = new KafkaProducer[GenericRecord, GenericRecord](kafkaProps)

  def produce(topic: String, key: K, value: V, partition: Int = 0)(implicit toRecordKey: ToRecord[K], fromRecordKey: FromRecord[K], toRecord: ToRecord[V], fromRecord: FromRecord[V]): Future[RecordMetadata] = {
    val keyRec = RecordFormat[K].to(key)
    val valueRec = RecordFormat[V].to(value)
    val data: ProducerRecord[GenericRecord, GenericRecord] = new ProducerRecord(topic, partition, keyRec, valueRec)
    producer.send(data)
  }

}