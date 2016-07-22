package eu.luminis.flink

import scopt.OptionParser
import java.util.Properties

object App {

  case class Config(
    topic: String = "test",
    servers: String = "localhost:9092",
    group: String = "test"
  )

  def main(args: Array[String]) {

    val parser = new OptionParser[Config]("scopt") {
      opt[String]('t', "topic").action((x, c) => c.copy(topic = x)).text("Topic to listen to")
      opt[String]('s', "servers").action((x, c) => c.copy(servers = x)).text("Kafka bootstrap servers")
      opt[String]('g', "group").action((x, c) => c.copy(servers = x)).text("Group id of the Kafka consumer")
    }

    parser.parse(args, Config()) match {
      case Some(config) =>
        new KafkaConsumer(config)

      case None =>
        println("Bad arguments")
    }

  }
}