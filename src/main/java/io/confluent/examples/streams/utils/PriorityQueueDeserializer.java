package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;
import java.util.PriorityQueue;

public class PriorityQueueDeserializer<T> implements Deserializer<PriorityQueue<T>> {

    /**
     * Constructor used by Kafka Streams.
     */
    public PriorityQueueDeserializer() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public PriorityQueue<T> deserialize(String s, byte[] bytes) {
        // placeholder that will not work
        return new PriorityQueue<>();
    }

    @Override
    public void close() {

    }
}
