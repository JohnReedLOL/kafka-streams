/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.examples.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Arrays;
import java.util.Properties;

/**
 * Demonstrates, using the high-level KStream DSL, how to implement the WordCount program that
 * computes a simple word occurrence histogram from an input text.  This example uses lambda
 * expressions and thus works with Java 8+ only.
 *
 * In this example, the input stream reads from a topic named "TextLinesTopic", where the values of
 * messages represent lines of text; and the histogram output is written to topic
 * "WordsWithCountsTopic", where each record is an updated count of a single word, i.e. `word
 * (String) -> currentCount (Long)`.
 *
 * Note: Before running this example you must 1) create the source topic (e.g. via `kafka-topics
 * --create ...`), then 2) start this example and 3) write some data to the source topic (e.g. via
 * `kafka-console-producer`). Otherwise you won't see any data arriving in the output topic.
 *
 *
 * HOW TO RUN THIS EXAMPLE
 *
 * 1) Start Zookeeper and Kafka. Please refer to <a href='http://docs.confluent.io/3.0.0/quickstart.html#quickstart'>CP3.0.0
 * QuickStart</a>.
 *
 * 2) Create the input and output topics used by this example.
 *
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic TextLinesTopic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic WordsWithCountsTopic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }
 * </pre>
 *
 * Note: The above commands are for CP 3.0.0 only. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 *
 * 3) Start this example application either in your IDE or on the command line.
 *
 * If via the command line please refer to <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 *
 * <pre>
 * {@code
 * $ java -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.WordCountLambdaExample
 * }
 * </pre>
 *
 * 4) Write some input data to the source topics (e.g. via `kafka-console-producer`.  The already
 * running example application (step 3) will automatically process this input data and write the
 * results to the output topic.
 *
 * <pre>
 * {@code
 * # Start the console producer.  You can then enter input data by writing some line of text,
 * # followed by ENTER:
 * #
 * #   hello kafka streams<ENTER>
 * #   all streams lead to kafka<ENTER>
 * #   join kafka summit<ENTER>
 * #
 * # Every line you enter will become the value of a single Kafka message.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
 * }
 * </pre>
 *
 * 5) Inspect the resulting data in the output topics, e.g. via `kafka-console-consumer`.
 *
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic WordsWithCountsTopic --from-beginning \
 *                              --zookeeper localhost:2181 \
 *                              --property print.key=true
 *                              --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }
 * </pre>
 *
 * You should see output data similar to:
 *
 * <pre>
 * {@code
 * hello    1
 * kafka    1
 * streams  1
 * all      1
 * streams  2
 * lead     1
 * to       1
 * kafka    2
 * join     1
 * kafka    3
 * summit   1
 * }
 * </pre>
 *
 * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
 * also stop the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance (`Ctrl-C`). *
 */
public class WordCountLambdaExample {

  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-lambda-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    // Set up serializers and deserializers, which we will use for overriding the default serdes
    // specified above.
    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    // In the subsequent lines we define the processing topology of the Streams application.
    KStreamBuilder builder = new KStreamBuilder();

    // Construct a `KStream` from the input topic "TextLinesTopic", where message values
    // represent lines of text (for the sake of this example, we ignore whatever may be stored
    // in the message keys).
    //
    // Note: We could also just call `builder.stream("TextLinesTopic")` if we wanted to leverage
    // the default serdes specified in the Streams configuration above, because these defaults
    // match what's in the actual topic.  However we explicitly set the deserializers in the
    // call to `stream()` below in order to show how that's done, too.
    KStream<String, String> textLines = builder.stream(stringSerde, stringSerde, "TextLinesTopic");

    KStream<String, Long> wordCounts = textLines
        // Split each text line, by whitespace, into words.  The text lines are the record
        // values, i.e. we can ignore whatever data is in the record keys and thus invoke
        // `flatMapValues` instead of the more generic `flatMap`.
        .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
        // We will subsequently invoke `countByKey` to count the occurrences of words, so we use
        // `map` to ensure the key of each record contains the respective word.
        .map((key, word) -> new KeyValue<>(word, word))
        // Count the occurrences of each word (record key).
        //
        // This will change the stream type from `KStream<String, String>` to
        // `KTable<String, Long>` (word -> count).  We must provide a name for
        // the resulting KTable, which will be used to name e.g. its associated
        // state store and changelog topic.
        .countByKey("Counts")
        // Convert the `KTable<String, Long>` into a `KStream<String, Long>`.
        .toStream();

    // Write the `KStream<String, Long>` to the output topic.
    wordCounts.to(stringSerde, longSerde, "WordsWithCountsTopic");

    // Now that we have finished the definition of the processing topology we can actually run
    // it via `start()`.  The Streams application as a whole can be launched just like any
    // normal Java application that has a `main()` method.
    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}
