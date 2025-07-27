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

import io.nats.client.api.OrderedConsumerConfiguration;

import java.io.IOException;
import java.io.Serializable;

public class SerializableOrderedConsumerConfiguration implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient OrderedConsumerConfiguration occ;

    public SerializableOrderedConsumerConfiguration() {
        setOrderedConsumerConfiguration(new OrderedConsumerConfiguration());
    }

    public SerializableOrderedConsumerConfiguration(OrderedConsumerConfiguration occ) {
        setOrderedConsumerConfiguration(occ);
    }

    public void setOrderedConsumerConfiguration(OrderedConsumerConfiguration occ) {
        this.occ = occ;
    }

    public OrderedConsumerConfiguration getOrderedConsumerConfiguration() {
        return occ;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(occ.toJson());
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        occ = new OrderedConsumerConfiguration(in.readUTF());
    }
}
