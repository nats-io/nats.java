// Copyright 2023 The NATS Authors
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

package io.nats.examples.testapp;

import io.nats.client.*;
import io.nats.client.api.OrderedConsumerConfiguration;
import io.nats.examples.testapp.support.CommandLine;
import io.nats.examples.testapp.support.ConsumerKind;

import java.io.IOException;

public class SimpleConsumer extends ConnectableConsumer {
    final StreamContext sc;
    final ConsumerContext cc;
    final OrderedConsumerContext occ;
    final MessageConsumer mc;

    public SimpleConsumer(CommandLine cmd, ConsumerKind consumerKind, int batchSize, long expiresIn) throws IOException, InterruptedException, JetStreamApiException {
        super(cmd, "sc", consumerKind);

        sc = nc.getStreamContext(cmd.stream);

        ConsumeOptions co = ConsumeOptions.builder()
            .batchSize(batchSize)
            .expiresIn(expiresIn)
            .build();

        if (consumerKind == ConsumerKind.Ordered) {
            OrderedConsumerConfiguration ocConfig = new OrderedConsumerConfiguration().filterSubjects(cmd.subject);
            cc = null;
            occ = sc.createOrderedConsumer(ocConfig);
            mc = occ.consume(co, handler);
        }
        else {
            occ = null;
            cc = sc.createOrUpdateConsumer(newCreateConsumer().build());
            mc = cc.consume(co, handler);
        }
        Ui.controlMessage(label, mc.getConsumerName());
    }
}
