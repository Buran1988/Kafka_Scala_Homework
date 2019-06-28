name := "homework_on_sbt"

version := "0.1"

scalaVersion := "2.12.0"
val confluentVersion = "5.2.1"

mainClass := Some("MainProg.scala")

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.3.0",
    // https://mvnrepository.com/artifact/org.apache.avro/avro
    "org.apache.avro" % "avro" % "1.9.0",

    // https://mvnrepository.com/artifact/io.confluent/kafka-avro-serializer
    "org.apache.kafka" % "kafka-clients" % "2.2.1",

    // https://mvnrepository.com/artifact/io.confluent/kafka-avro-serializer
    "io.confluent" % "kafka-avro-serializer" % "3.3.1",


    // https://mvnrepository.com/artifact/com.sksamuel.avro4s/avro4s-core
    "com.sksamuel.avro4s" %% "avro4s-core" % "1.9.0",
    // https://mvnrepository.com/artifact/org.codehaus.jackson/jackson-mapper-asl
    "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13",
        "com.github.nscala-time" %% "nscala-time" % "2.22.0",

    "org.apache.parquet" % "parquet-avro" % "1.10.1",
    "org.apache.hadoop" % "hadoop-common" % "3.2.0",
    "org.apache.hadoop" % "hadoop-hdfs" % "3.2.0"
  )
}





resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)
