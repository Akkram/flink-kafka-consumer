package eu.luminis.flink

import java.util.Properties
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.util.serialization.{ DeserializationSchema, SerializationSchema }
import eu.luminis.flink.App.Config

class KafkaConsumer(config: Config) {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties();
  properties.setProperty("bootstrap.servers", config.servers);
  properties.setProperty("group.id", config.group);


  val stream: DataStream[String] = env
    .addSource(new FlinkKafkaConsumer09[String](config.topic, 
        new SimpleStringSchema(), properties))

  stream
    .map((s: String) => s"This is a string: $s")
    .print

  env.execute("Flink Scala Kafka Consumer")

}
