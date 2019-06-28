package argument_support


case class ApplicationArguments(brokers: String, schema: String, topic: String, timeframe: Int, path_to_parquet: String)

trait ArgumentsSupport {

  val usage =
    """
      | kafka-homework-demo - run with arguments:
      |
      |  -brokers  127.0.0.1:9092
      |  -schema   http://127.0.0.1:8081
      |  -topic    data-sensor-topic
      |  -timeframe 5 (in seconds)
      |  -path_to_parquet C:/tmp
      |
      |
    """.stripMargin

  def parseArgumentsOrFail(args: Array[String]): ApplicationArguments = {
    if (args.length < 4) {
      println(usage)
      sys.exit(1)
    }
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap =
      list match {
        case Nil => map
        // PARAMETERS
        case "-brokers" :: value :: tail =>
          nextOption(map ++ Map('brokers -> value.toString), tail)
        case "-schema" :: value :: tail =>
          nextOption(map ++ Map('schema -> value.toString), tail)
        case "-topic" :: value :: tail =>
          nextOption(map ++ Map('topic -> value.toString), tail)
        case "-timeframe" :: value :: tail =>
          nextOption(map ++ Map('timeframe -> value.toString), tail)
        case "-path_to_parquet" :: value :: tail =>
          nextOption(map ++ Map('path_to_parquet -> value.toString), tail)

        case option :: tail => println("Unknown option " + option)
          nextOption(map, tail)
        // sys.exit(1)
      }

    val options: Map[Symbol, Any] = nextOption(Map(), arglist)
    validateArguments(options)

    val applicationArgs = ApplicationArguments(
      brokers = options('brokers).toString,
      schema = options('schema).toString,
      topic = options('topic).toString,
      timeframe = java.lang.Integer.valueOf(options('timeframe).toString),
      path_to_parquet = options('path_to_parquet).toString
    )

    println(applicationArgs)
    applicationArgs

  }

  def validateArguments(options: Map[Symbol, Any]) = {
    val requiredConfigs = List('brokers, 'schema, 'topic, 'timeframe, 'path_to_parquet)

    requiredConfigs.foreach { symbol =>
      if (!options.contains(symbol)) {
        println(s"You need to supply the argument : $symbol")
        println(usage)
        sys.exit(1)
      }
    }
  }
}

