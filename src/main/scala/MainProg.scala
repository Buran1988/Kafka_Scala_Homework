import argument_support.{ApplicationArguments, ArgumentsSupport}
import consumer_logic.KConsumer
import producer_logic.ProduceMessages

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


object MainProg extends App with ArgumentsSupport {


  override def main(args: Array[String]): Unit = {
    val APP_ARGS: ApplicationArguments = parseArgumentsOrFail(args)
    val welcome =
      """
        | Starting Kafka Homework Demo Application
        |
        | This application does the following
        |  1) Produces Avro messages in background
        |  2) Consumes the messages in a given timeframe
        |  3) Dumps the messages to local disk in Parquet files
        |
        |
        | Author: N.Kalutskiy
        | Date: 28.06.2019
        | Version: 1.0
        |
    """
    println(welcome)

    val workshop1: ProduceMessages = new ProduceMessages(APP_ARGS)
    Future {

      workshop1.runDevice()
    }

    new KConsumer(APP_ARGS)
      .consumeRecords()

  }


}
