// Copyright 2024 The NATS Authors
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

package io.nats.client.support;

import io.nats.client.ConsumeOptions;

import java.io.IOException;
import java.io.Serializable;

import static io.nats.client.ConsumeOptions.DEFAULT_CONSUME_OPTIONS;

public class SerializableConsumeOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient ConsumeOptions co;

    public SerializableConsumeOptions() {
        setConsumeOptions(DEFAULT_CONSUME_OPTIONS);
    }

    public SerializableConsumeOptions(ConsumeOptions co) {
        setConsumeOptions(co);
    }

    public SerializableConsumeOptions(ConsumeOptions.Builder builder) {
        setConsumeOptions(builder.build());
    }

    public void setConsumeOptions(ConsumeOptions co) {
        this.co = co;
    }

    public ConsumeOptions getConsumeOptions() {
        return co;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(co.toJson());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        co = ConsumeOptions.builder().json(in.readUTF()).build();
    }
}
