/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.examples.streams;

import io.confluent.examples.streams.avro.WikiFeed;
import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.Properties;


/**
 * Computes, for every minute the number of new user feeds from the Wikipedia feed irc stream.
 * Same as WikipediaFeedAvroLambdaExample but does not use lambda expressions and thus works on
 * Java 7+.
 *
 * Note: The specific Avro binding is used for serialization/deserialization, where the `WikiFeed`
 * class is auto-generated from its Avro schema by the maven avro plugin.  See `wikifeed.avsc`
 * under `src/main/avro/`.
 */
public class WikipediaFeedAvroExample {

    public static void main(String[] args) throws Exception {
        Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-avro-example");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        // Where to find the Confluent schema registry instance(s)
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        KStreamBuilder builder = new KStreamBuilder();

        // read the source stream
        KStream<String, WikiFeed> feeds = builder.stream("WikipediaFeed");

        // aggregate the new feed counts of by user
        KTable<String, Long> aggregated = feeds
                // filter out old feeds
                .filter(new Predicate<String, WikiFeed>() {
                    @Override
                    public boolean test(String dummy, WikiFeed value) {
                        return value.getIsNew();
                    }
                })
                // map the user id as key
                .map(new KeyValueMapper<String, WikiFeed, KeyValue<String, WikiFeed>>() {
                    @Override
                    public KeyValue<String, WikiFeed> apply(String key, WikiFeed value) {
                        return new KeyValue<>(value.getUser(), value);
                    }
                })
                // sum by key, need to override the serdes for String typed key
                .countByKey(stringSerde, "Counts");

        // write to the result topic, need to override serdes
        aggregated.to(stringSerde, longSerde, "WikipediaStats");

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();
    }
}