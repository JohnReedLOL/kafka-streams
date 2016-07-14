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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Demonstrates how to use `reduceByKey` to sum numbers. See `SumLambdaIntegrationTest` for an
 * end-to-end example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
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
 * $ bin/kafka-topics --create --topic numbers-topic \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic sum-of-odd-numbers-topic \
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
 * $ java -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.SumLambdaExample
 * }
 * </pre>
 *
 * 4) Write some input data to the source topic (e.g. via {@link SumLambdaExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic.
 *
 * <pre>
 * {@code
 * # Here: Write input data using the example driver.  Once the driver has stopped generating data,
 * # you can terminate it via `Ctrl-C`.
 * $ java -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.SumLambdaExampleDriver
 * }
 * </pre>
 *
 * 5) Inspect the resulting data in the output topics, e.g. via `kafka-console-consumer`.
 *
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic sum-of-odd-numbers-topic --from-beginning
 *        --zookeeper localhost:2181 \
 *        --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer
 * }
 * </pre>
 *
 * You should see output data similar to:
 *
 * <pre>
 * {@code
 *    1
 *    4
 *    9
 *   16
 *   25
 *   36
 *   49
 *  ...
 * 2209
 * 2304
 * 2401
 * 2500
 * }
 * </pre>
 *
 * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
 * also stop the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance (`Ctrl-C`).
 */
public class SumLambdaExample {

  static final String SUM_OF_ODD_NUMBERS_TOPIC = "sum-of-odd-numbers-topic";
  static final String NUMBERS_TOPIC = "numbers-topic";

  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-lambda-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

    KStreamBuilder builder = new KStreamBuilder();
    // We assume the input topic contains records where the values are Integers.
    // We don't really care about the keys of the input records;  for simplicity, we assume them
    // to be Integers, too, because we will re-key the stream later on, and the new key will be
    // of type Integer.
    KStream<Integer, Integer> input = builder.stream(NUMBERS_TOPIC);
    KTable<Integer, Integer> sumOfOddNumbers = input
        // We are only interested in odd numbers.
        .filter((k, v) -> v % 2 != 0)
        // We want to compute the total sum across ALL numbers, so we must re-key all records to the
        // same key.  This re-keying is required because in Kafka Streams a data record is always a
        // key-value pair, and KStream aggregations such as `reduceByKey` operate on a per-key basis.
        // The actual new key (here: `1`) we pick here doesn't matter as long it is the same across
        // all records.
        .selectKey((k, v) -> 1)
        // Add the numbers to compute the sum.
        .reduceByKey((v1, v2) -> v1 + v2, "sum");
    sumOfOddNumbers.to(SUM_OF_ODD_NUMBERS_TOPIC);

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}