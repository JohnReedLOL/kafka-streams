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
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

/**
 * Computes, per region, the number of users with "complete" user profiles for such regions that
 * have at least 10 million users with complete profiles.  A user profile is naively considered
 * "complete" whenever it has a total of at least 200 characters.
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
 * $ bin/kafka-topics --create --topic UserRegions \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic LargeRegions \
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
 * $ java -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.UserRegionLambdaExample
 * }
 * </pre>
 *
 * 4) Write some input data to the source topics (e.g. via `kafka-console-producer`.  The already
 * running example application (step 3) will automatically process this input data and write the
 * results to the output topic.
 *
 * <pre>
 * {@code
 * # Start the console producer, then input some example data records.  The input data you enter
 * # should be in the form of USER,REGION<ENTER> and, because this example is set to discard any
 * # regions that have a user count of only 1, at least one region should have two users or more --
 * # otherwise this example won't produce any output data (cf. step 5).
 * #
 * # alice,asia<ENTER>
 * # bob,americas<ENTER>
 * # chao,asia<ENTER>
 * # dave,europe<ENTER>
 * # alice,europe<ENTER>        <<< Note: Alice moved from Asia to Europe
 * # eve,americas<ENTER>
 * # fang,asia<ENTER>
 * # gandalf,europe<ENTER>
 * #
 * # Here, the part before the comma will become the message key, and the part after the comma will
 * # become the message value.
 * $ bin/kafka-console-producer --broker-list localhost:9092 --topic UserRegions \
 *                              --property parse.key=true --property key.separator=,
 * }
 * </pre>
 *
 * 5) Inspect the resulting data in the output topics, e.g. via `kafka-console-consumer`.
 *
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --topic LargeRegions --from-beginning \
 *        --zookeeper localhost:2181 \
 *        --property print.key=true \
 *        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }
 * </pre>
 *
 * You should see output data similar to:
 *
 * <pre>
 * {@code
 * asia     2     # because Alice and Chao are currently in Asia
 * europe   2     # because Dave and Alice (who moved from Asia to Europe) are currently in Europe
 * americas 2     # because Bob and Eve are currently in Americas
 * asia     2     # because Chao and Fang are currently in Asia
 * europe   3     # because Dave, Alice, and Gandalf are currently in Europe
 * }
 * </pre>
 *
 * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
 * also stop the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance (`Ctrl-C`).
 */
public class UserRegionLambdaExample {

  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "user-region-lambda-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    KStreamBuilder builder = new KStreamBuilder();

    // Read the source stream
    // We assume record key = username and record value = geo-region
    KTable<String, String> userRegions = builder.table("UserRegions");

    // Aggregate the user counts of by region
    KTable<String, Long> regionCounts = userRegions
        // Count by region
        // We do not need to specify any explict serdes because the key and value types do not change
        .groupBy((userId, region) -> KeyValue.pair(region, region))
        .count("CountsByRegion")
        // discard any regions with only 1 user
        .filter((regionName, count) -> count >= 2);

    // Note: The following operations would NOT be needed for the actual users-per-region
    // computation, which would normally stop at the filter() above.  We use the operations
    // below only to "massage" the output data so it is easier to inspect on the console via
    // kafka-console-consumer.
    //
    KStream<String, Long> regionCountsForConsole = regionCounts
        // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
        .toStream()
        // sanitize the output by removing null record values (again, we do this only so that the
        // output is easier to read via kafka-console-consumer combined with LongDeserializer
        // because LongDeserializer fails on null values, and even though we could configure
        // kafka-console-consumer to skip messages on error the output still wouldn't look pretty)
        .filter((regionName, count) -> count != null);

    // write to the result topic, we need to override the value serializer to for type long
    regionCountsForConsole.to(stringSerde, longSerde, "LargeRegions");

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}