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
package io.confluent.examples.streams.utils;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;

import java.nio.ByteBuffer;
import java.util.Map;

public class WindowedStringDeserializer implements Deserializer<Windowed<String>> {

    private static final int TIMESTAMP_SIZE = 8;
    private StringDeserializer keyDeserializer = new StringDeserializer();
    @Override
    public void configure(final Map configs, final boolean isKey) { }

    @Override
    public Windowed<String> deserialize(final String topic, final byte[] data) {
        byte[] bytes = new byte[data.length - TIMESTAMP_SIZE];
        System.arraycopy(data, 0, bytes, 0, bytes.length);
        long start = ByteBuffer.wrap(data).getLong(data.length - TIMESTAMP_SIZE);
        return new Windowed<>(keyDeserializer.deserialize(topic, bytes), new UnlimitedWindow(start));
    }

    @Override
    public void close() {

    }
}
