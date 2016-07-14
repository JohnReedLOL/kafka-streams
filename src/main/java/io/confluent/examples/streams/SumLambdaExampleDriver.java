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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * This is a sample driver for the {@link SumLambdaExample}.
 * To run this driver please first refer to the instructions in {@link SumLambdaExample}.
 * You can then run this class directly in your IDE or via the command line.
 *
 * To run via the command line you might want to package as a fatjar first. Please refer to:
 * <a href='https://github.com/confluentinc/examples/tree/master/kafka-streams#packaging-and-running'>Packaging</a>
 *
 * Once packaged you can then run:
 * java -cp target/streams-examples-3.0.0-standalone.jar io.confluent.examples.streams.SumLambdaExampleDriver
 *
 * You should terminate with Ctrl-C
 * Please refer to {@link SumLambdaExample} for instructions on running the example.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class SumLambdaExampleDriver {

  public static void main(String[] args) throws Exception {
    produceInput();
    consumeOutput();
  }

  private static void consumeOutput() {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG,
                           "sum-lambda-example-consumer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaConsumer<Integer, Integer> consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Collections.singleton(SumLambdaExample.SUM_OF_ODD_NUMBERS_TOPIC));
    while (true) {
      ConsumerRecords<Integer, Integer> records =
          consumer.poll(Long.MAX_VALUE);

      for (ConsumerRecord<Integer, Integer> record : records) {
        System.out.println("Current sum of odd numbers is:" + record.value());
      }
    }
  }

  private static void produceInput() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);

    final KafkaProducer<Integer, Integer> producer = new KafkaProducer<>(props);

    IntStream.range(0, 100)
        .mapToObj(val -> new ProducerRecord<>(SumLambdaExample.NUMBERS_TOPIC, val, val))
        .forEach(producer::send);

    producer.flush();
  }


}