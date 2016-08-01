
import java.util.concurrent.TimeUnit
import java.util.{Properties, UUID}

import com.datastax.driver.core.utils.UUIDs
import com.sksamuel.kafka.embedded.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.JavaConverters._

class TrendServiceIntegrationSpec extends FlatSpec with MockFactory with BeforeAndAfter with Matchers {
  var kafka: EmbeddedKafka = _
  var kafkaConfig: EmbeddedKafkaConfig = _

  val topic = "testing"
  val sampleBody = "Sample Tweet Body Text"
  val alertId = UUIDs.timeBased()

  before {
    kafkaConfig = EmbeddedKafkaConfig()
    kafka = new EmbeddedKafka(kafkaConfig)
    kafka.start()
    _fillKafkaTopicWithSampleStatuses(sampleBody, 10, 100)
  }

  after {
    kafka.stop()
  }

  "Integration" should "integrate" in {

  }


  def _fillKafkaTopicWithSampleStatuses(word: String, occurence: Int, totalStatusCount: Int) {
    val producer = _generateKafkaProducer(kafkaConfig)

    (1 to occurence)
      .toList
      .foreach { p => producer.send(new ProducerRecord[Long, String](topic, p, word)) }

    (occurence until totalStatusCount)
      .toList
      .foreach { p => producer.send(new ProducerRecord[Long, String](topic, p, generateRandomWord())) }

    producer.close(1, TimeUnit.MINUTES)
  }

  def generateRandomWord(): String = {
    val r = new scala.util.Random(100)
    val x = for (i <- 0 to 10) yield r.nextPrintableChar
    x.foldLeft("")((a, b) => a + b)
  }

  def _generateKafkaProducer(conf: EmbeddedKafkaConfig): KafkaProducer[Long, String] = {
    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + conf.kafkaPort)
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[Long, String](producerProps)
  }

  def _generateKafkaConsumer(conf: EmbeddedKafkaConfig): KafkaConsumer[Long, String] = {
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