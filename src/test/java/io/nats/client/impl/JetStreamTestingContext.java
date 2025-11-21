// Copyright 2025 The NATS Authors
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

package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.api.StreamInfo;
import io.nats.client.utils.TestBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JetStreamTestingContext implements AutoCloseable {
    public final NatsJetStreamManagement jsm;
    public final NatsJetStream js;
    public final String stream;
    public StreamInfo si;

    private final String subjectBase;
    private final Map<Object, String> subjects;
    private final String consumerNameBase;
    private final Map<Object, String> consumerNames;

    public JetStreamTestingContext(Connection nc) throws JetStreamApiException, IOException {
        this(nc, 1);
    }

    public JetStreamTestingContext(Connection nc, int subjectCount) throws JetStreamApiException, IOException {
        this.jsm = (NatsJetStreamManagement)nc.jetStreamManagement();
        this.js = (NatsJetStream)nc.jetStream();
        stream = TestBase.random();
        subjectBase = TestBase.random();
        this.subjects = new HashMap<>();
        consumerNameBase = TestBase.random();
        this.consumerNames = new HashMap<>();

        subjectCount = Math.max(subjectCount, 1);
        String[] subjects = new String[subjectCount];
        for (int x = 0; x < subjectCount; x++) {
            subjects[x] = subject(x);
        }
        this.si = TestBase.createMemoryStream(jsm, stream, subjects);
    }

    @Override
    public void close() throws Exception {
        try {
            jsm.deleteStream(stream);
        }
        catch (Exception ignore) {
        }
    }

    public String subject() {
        return subject(0);
    }

    public String subject(Object variant) {
        return subjects.computeIfAbsent(variant, v -> subjectBase + "-" + v);
    }

    public String consumerName() {
        return consumerNameBase;
    }

    public String consumerName(Object variant) {
        return consumerNames.computeIfAbsent(variant, v -> consumerNameBase + "-" + v);
    }
}
