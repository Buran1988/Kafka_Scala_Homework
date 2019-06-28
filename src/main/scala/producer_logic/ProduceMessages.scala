package producer_logic

import java.util.{Timer, TimerTask}
import argument_support.ApplicationArguments
import models.{DeviceMeasurements, Key}
import com.github.nscala_time.time.Imports._

import scala.util.{Failure, Random, Success, Try}

class ProduceMessages(APP_ARGS: ApplicationArguments) {

  def runDevice(): Unit = {
    val t = new Timer()
    t.schedule(new TimerTask() {
      @Override
      def run() {
        println("posting")
        val producer = new KProducer[Key, DeviceMeasurements](APP_ARGS)
        produceMessages(Random.nextInt(100), producer)
      }
    }, 0, 10000)
  }


  private def produceMessages(numberOfMessages: Int, producer: KProducer[Key, DeviceMeasurements]): Unit = {
    for (a <- 1 to numberOfMessages) {

      val deviceID = getMeasurement(0, 10)

      val date = Integer.valueOf(DateTime.now().toString("yyyyMMdd"))
      val time = Integer.valueOf(DateTime.now().toString("hhmmss"))
      val key = DateTime.now().getMillis
      val value = Random.nextFloat()
      val valueType = "demo_measurement"

      Try(producer.produce(APP_ARGS.topic, Key(deviceID), DeviceMeasurements(date, time, key, value, valueType))) match {
        case Success(m) =>

          val metadata = m.get()
          println("Success writing to Kafka topic:" + metadata.topic(),
            " offset: " + metadata.offset(),
            " partition: " + metadata.partition(),
            " timestamp: " + new DateTime(metadata.timestamp()))

        case Failure(f) => println("Failed writing to Kafka", f)
      }
    }
  }


  private def getMeasurement(min: Int, max: Int) = Random.nextInt(max - min) + min
}
