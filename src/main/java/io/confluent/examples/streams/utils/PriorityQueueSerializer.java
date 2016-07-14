package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.PriorityQueue;

public class PriorityQueueSerializer<T> implements Serializer<PriorityQueue<T>> {

    /**
     * Constructor used by Kafka Streams.
     */
    public PriorityQueueSerializer() {

    }
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(String topic, PriorityQueue<T> record) {
        // placeholder that will not work
        return new byte[0];
    }

    @Override
    public void close() {

    }
}
