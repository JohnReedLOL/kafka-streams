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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;

import java.io.InputStream;
import java.util.Properties;

import io.confluent.examples.streams.utils.GenericAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

/**
 * Demonstrates how to perform a join between a KStream and a KTable, i.e. an example of a stateful
 * computation, using the generic Avro binding for serdes in Kafka Streams. Same as
 * PageViewRegionLambdaExample but does not use lambda expressions and thus works on Java 7+.
 *
 * In this example, we join a stream of page views (aka clickstreams) that reads from a topic named
 * "PageViews" with a user profile table that reads from a topic named "UserProfiles" to compute the
 * number of page views per user region.
 *
 * Note: The generic Avro binding is used for serialization/deserialization.  This means the
 * appropriate Avro schema files must be provided for each of the "intermediate" Avro classes, i.e.
 * whenever new types of Avro objects (in the form of GenericRecord) are being passed between
 * processing steps.
 *
 * HOW TO RUN THIS EXAMPLE
 *
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a
 * href='http://docs.confluent.io/3.0.0/quickstart.html#quickstart'>CP3.0.0 QuickStart</a>.
 *
 * 2) Create the input/intermediate/output topics used by this example.
 *
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic PageViews \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic PageViewsByUser \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic UserProfiles \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ bin/kafka-topics --create --topic PageViewsByRegion \
 *                    --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }
 * </pre>*
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
 * $ java -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.PageViewRegionLambdaExample
 * }
 * </pre>
 *
 * 4) Write some input data to the source topics (e.g. via {@link PageViewRegionExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic.
 *
 * <pre>
 * {@code
 * # Here: Write input data using the example driver.  Once the driver has stopped generating data,
 * # you can terminate it via `Ctrl-C`.
 * $ java -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.PageViewRegionExampleDriver
 * }
 * </pre>
 *
 * 5) Inspect the resulting data in the output topic, e.g. via `kafka-console-consumer`.
 *
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer --zookeeper localhost:2181 --topic PageViewsByRegion
 * --from-beginning \
 *          --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }
 * </pre>
 *
 * You should see output data similar to:
 *
 * <pre>
 * {@code
 * [africa@1466515140000]  1
 * [africa@1466515080000]  1
 * [africa@1466514900000]  1
 * [africa@1466515020000]  1
 * [africa@1466514960000]  1
 * [africa@1466514900000]  2
 * [africa@1466514960000]  2
 * [africa@1466515020000]  2
 * [africa@1466515080000]  2
 * [africa@1466515140000]  2
 * [asia@1466515140000]  1
 * [asia@1466515080000]  1
 * [asia@1466514900000]  1
 * [asia@1466515020000]  1
 * [asia@1466514960000]  1
 * [asia@1466514900000]  2
 * [asia@1466514960000]  2
 * [asia@1466515020000]  2
 * [asia@1466515080000]  2
 * [asia@1466515140000]  2
 * [asia@1466514900000]  3
 * ...
 * }
 * </pre>
 *
 * Here, the output format is "[REGION@WINDOW_START_TIME] COUNT".
 *
 * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
 * also stop the Confluent Schema Registry (`Ctrl-C`), then stop the Kafka broker (`Ctrl-C`), and
 * only then stop the ZooKeeper instance (`Ctrl-C`).
 */
public class PageViewRegionExample {

  public static void main(String[] args) throws Exception {
    Properties streamsConfiguration = new Properties();
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "pageview-region-example");
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // Where to find the corresponding ZooKeeper ensemble.
    streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
    // Where to find the Confluent schema registry instance(s)
    streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    // Specify default (de)serializers for record keys and for record values.
    streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    final Serde<String> stringSerde = Serdes.String();
    final Serde<Long> longSerde = Serdes.Long();

    KStreamBuilder builder = new KStreamBuilder();

    // Create a stream of page view events from the PageViews topic, where the key of
    // a record is assumed to be the user id (String) and the value an Avro GenericRecord
    // that represents the full details of the page view event.  See `pageview.avsc` under
    // `src/main/avro/` for the corresponding Avro schema.
    KStream<String, GenericRecord> views = builder.stream("PageViews");

    KStream<String, GenericRecord> viewsByUser = views.map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
      @Override
      public KeyValue<String, GenericRecord> apply(String dummy, GenericRecord record) {
        return new KeyValue<>(record.get("user").toString(), record);
      }
    }).through("PageViewsByUser");

    // Create a changelog stream for user profiles from the UserProfiles topic,
    // where the key of a record is assumed to be the user id (String) and its value
    // an Avro GenericRecord.  See `userprofile.avsc` under `src/main/avro/` for the
    // corresponding Avro schema.
    KTable<String, GenericRecord> userProfiles = builder.table("UserProfiles");

    KTable<String, String> userRegions = userProfiles.mapValues(new ValueMapper<GenericRecord, String>() {
      @Override
      public String apply(GenericRecord record) {
        return record.get("region").toString();
      }
    });

    // We must specify the Avro schemas for all intermediate (Avro) classes, if any.
    // In this example, we want to create an intermediate GenericRecord to hold the view region
    // (see below).
    final InputStream
        pageViewRegionSchema =
        PageViewRegionLambdaExample.class.getClassLoader()
            .getResourceAsStream("avro/io/confluent/examples/streams/pageviewregion.avsc");
    Schema schema = new Schema.Parser().parse(pageViewRegionSchema);

    KTable<Windowed<String>, Long> viewsByRegion = viewsByUser
        .leftJoin(userRegions, new ValueJoiner<GenericRecord, String, GenericRecord>() {
          @Override
          public GenericRecord apply(GenericRecord view, String region) {
            GenericRecord viewRegion = new GenericData.Record(schema);
            viewRegion.put("user", view.get("user"));
            viewRegion.put("page", view.get("page"));
            viewRegion.put("region", region);
            return viewRegion;
          }
        })
        .map(new KeyValueMapper<String, GenericRecord, KeyValue<String, GenericRecord>>() {
          @Override
          public KeyValue<String, GenericRecord> apply(String user, GenericRecord viewRegion) {
            return new KeyValue<>(viewRegion.get("region").toString(), viewRegion);
          }
        })
        // count views by user, using hopping windows of size 5 minutes that advance every 1 minute
        .countByKey(TimeWindows.of("GeoPageViewsWindow", 5 * 60 * 1000L).advanceBy(60 * 1000L));

    // Note: The following operations would NOT be needed for the actual pageview-by-region
    // computation, which would normally stop at the countByKey() above.  We use the operations
    // below only to "massage" the output data so it is easier to inspect on the console via
    // kafka-console-consumer.
    KStream<String, Long> viewsByRegionForConsole = viewsByRegion
        // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
        // and by also converting the record key from type `Windowed<String>` (which
        // kafka-console-consumer can't print to console out-of-the-box) to `String`
        .toStream(new KeyValueMapper<Windowed<String>, Long, String>() {
          @Override
          public String apply(Windowed<String> windowedRegion, Long count) {
            return windowedRegion.toString();
          }
        });

    // write to the result topic
    viewsByRegionForConsole.to(stringSerde, longSerde, "PageViewsByRegion");

    KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
    streams.start();
  }

}
