// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package io.nats.client;

import io.nats.client.impl.Headers;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public interface Deserializer extends Closeable, Serializable {

    default void configure(Map<String, ?> configs, boolean isKey) {
    }

    byte[] decode(String var1, byte[] var2) throws IOException;

    default byte[] decode(String topic, Headers headers, byte[] data) throws IOException {
        return this.decode(topic, data);
    }

    default void close() throws IOException{
    }
}
