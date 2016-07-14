/*Property of Accenture*/

package io.confluent.examples.streams

import java.lang.{Long => JLong}
import java.util.Properties

import io.confluent.examples.streams.kafka.EmbeddedSingleNodeKafkaCluster
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig}
import org.assertj.core.api.Assertions.assertThat
import org.junit._
import org.scalatest.junit.AssertionsForJUnit

import scala.xml.Elem
// import scala.trace._

/**
  * End-to-end integration test based on [[WordCountLambdaExample]], using an embedded Kafka cluster.
  *
  * See [[WordCountLambdaExample]] for further documentation.
  *
  * See [[WordCountLambdaIntegrationTest]] for the equivalent Java example.
  *
  * Note: We intentionally use JUnit4 (wrapped by ScalaTest) for implementing this Scala integration
  * test so it is easier to compare this Scala code with the equivalent Java code at
  * JoinLambdaIntegrationTest.  One difference is that, to simplify the Scala/Junit integration, we
  * switched from BeforeClass (which must be `static`) to Before as well as from @ClassRule (which
  * must be `static` and `public`) to a workaround combination of `@Rule def` and a `private val`.
  */
class ScalaProofOfConcept extends AssertionsForJUnit {

  private val privateCluster: EmbeddedSingleNodeKafkaCluster = new EmbeddedSingleNodeKafkaCluster

  @Rule def CLUSTER = privateCluster

  private val inputTopic = "inputTopic"
  private val outputTopic = "output-topic"

  @Before
  def startKafkaCluster() = {
    CLUSTER.createTopic(inputTopic)
    CLUSTER.createTopic(outputTopic)
  }

  @Test
  def shouldCountClicksPerRegion() {
    // To convert between Scala's `Tuple2` and Streams' `KeyValue`.
    import KeyValueImplicits._

    val hello = "Hello"
    val kafka = "Kafka"

    val toConvert: Elem =
      <myxml>
        <field>{hello}</field>
        <field>{kafka}</field>
      </myxml>

    println("XML: " + toConvert)

    val inputTextLines: Seq[String] = Seq(
      "Hello Kafka Streams",
      "All streams lead to Kafka",
      "Join Kafka Summit"
    )

    val expectedWordCounts: Seq[KeyValue[String, Long]] = Seq(
      ("hello", 1L),
      ("kafka", 1L),
      ("streams", 1L),
      ("all", 1L),
      ("streams", 2L),
      ("lead", 1L),
      ("to", 1L),
      ("kafka", 2L),
      ("join", 1L),
      ("kafka", 3L),
      ("summit", 1L)
    )

    //
    // Step 1: Configure and start the processor topology.
    //
    val streamsConfiguration: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-scala-integration-test")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
      p.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect())
      p.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      // Explicitly place the state directory under /tmp so that we can remove it via
      // `purgeLocalStreamsState` below.  Once Streams is updated to expose the effective
      // StreamsConfig configuration (so we can retrieve whatever state directory Streams came up
      // with automatically) we don't need to set this anymore and can update `purgeLocalStreamsState`
      // accordingly.
      p.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams")
      p
    }

    // Remove any state from previous test runs
    IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration)

    val stringSerde: Serde[String] = Serdes.String()
    val longSerde: Serde[JLong] = Serdes.Long()

    val builder: KStreamBuilder = new KStreamBuilder()

    // Construct a `KStream` from the input topic, where message values represent lines of text (for
    // the sake of this example, we ignore whatever may be stored in the message keys).
    val textLines: KStream[String, String] = builder.stream(inputTopic)

    // Scala-Java interoperability: to convert `scala.collection.Iterable` to  `java.util.Iterable`
    // in `flatMapValues()` below.
    import collection.JavaConverters.asJavaIterableConverter

    val wordCounts: KStream[String, JLong] = textLines
      .flatMapValues(value => value.toLowerCase.split("\\W+").toIterable.asJava)
      .map((key, word) => new KeyValue(word, word))
      .countByKey("Counts")
      .toStream()

    wordCounts.to(stringSerde, longSerde, outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
    streams.start()

    //
    // Step 2: Publish some user click events.
    //
    val producerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p
    }
    // Scala-Java interoperability: to convert `inputTextLines, `expectedWordCounts` (which have
    // type `Seq`) to the appropriate Java collections, which we achieve via this import.
    // (We wouldn't need to convert if we modified the Java-focused `IntegrationTestUtils` to be
    // more Scala friendly or if we provided Scala-focused test utilities.)
    import scala.collection.convert.wrapAsJava._
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputTextLines, producerConfig)

    //
    // Step 3: Verify the application's output data.
    //
    val consumerConfig = {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-scala-integration-test-standard-consumer")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
      p
    }
    val actualWordCounts: java.util.List[KeyValue[String, Long]] =
      IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedWordCounts.size)
    assertThat(actualWordCounts).containsExactlyElementsOf(expectedWordCounts)
  }

}