
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.utils.UUIDs
import com.sksamuel.kafka.embedded.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class TwitterProcessorSpec extends FlatSpec with MockFactory with BeforeAndAfter with Matchers {
  var kafka: EmbeddedKafka = _
  var producer: KafkaProducer[Long, String] = _
  var kafkaConfig: EmbeddedKafkaConfig = _
  var consumer: KafkaConsumer[Long, String] = _

  val topic = "testing"
  val sampleBody = "Sample Tweet Body Text"
  val alertId = UUIDs.timeBased()
  val pollTimeout: Long = 3000

  before {
    kafkaConfig = EmbeddedKafkaConfig()
    kafka = new EmbeddedKafka(kafkaConfig)
    kafka.start()
    producer = generateKafkaProducer(kafkaConfig)
    consumer = generateKafkaConsumer(kafkaConfig)
    fillKafkaTopicWithSampleStatuses(sampleBody, 3, 3, producer)
  }

  after {
    producer.close(1, TimeUnit.MINUTES)
    consumer.close()
    kafka.stop()
  }

  "Kafka Consumer" should "finds 3 tweets related with alert" in {
    val records: ConsumerRecords[Long, String] = consumer.poll(pollTimeout)
    assert(records.count() == 3)
  }

  def fillKafkaTopicWithSampleStatuses(word: String, occurence: Int, totalStatusCount: Int, producer: KafkaProducer[Long, String]) {
    (1 to occurence)
      .toList
      .foreach { p => producer.send(new ProducerRecord[Long, String](topic, p, word)) }

    (occurence until totalStatusCount)
      .toList
      .foreach { p => producer.send(new ProducerRecord[Long, String](topic, p, generateRandomWord())) }

  }

  def generateRandomWord(): String = {
    val r = new scala.util.Random(100)
    val x = for (i <- 0 to 10) yield r.nextPrintableChar
    x.foldLeft("")((a, b) => a + b)
  }

  def generateKafkaProducer(conf: EmbeddedKafkaConfig): KafkaProducer[Long, String] = {
    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + conf.kafkaPort)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[Long, String](producerProps)
  }

  def generateKafkaConsumer(conf: EmbeddedKafkaConfig): KafkaConsumer[Long, String] = {
    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + conf.kafkaPort)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "myconsumer")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    val consumer = new KafkaConsumer[Long, String](consumerProps)
    consumer.subscribe(List(topic).asJava)
    consumer
  }

}