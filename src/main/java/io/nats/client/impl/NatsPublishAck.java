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
package io.nats.client.impl;

import io.nats.client.PublishAck;
import io.nats.client.impl.JsonUtils.FieldType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Internal class to handle JetStream acknowledgements
class NatsPublishAck implements PublishAck {

    private String stream = null;
    private long seq = -1;
    private boolean duplicate = false;

    private static final Pattern streamRE = JsonUtils.buildPattern("stream", FieldType.jsonString);
    private static final Pattern duplicateRE = JsonUtils.buildPattern("duplicate", FieldType.jsonBoolean);
    private static final Pattern seqnoRE = JsonUtils.buildPattern("seq", FieldType.jsonNumber); 

    // Acks will be received with the following format:
    // Error = {"code" : "description"}
    // OK = {"stream" : "mystream", "duplicate" : false, "seq", 42}"
    public NatsPublishAck(byte[] response) throws IOException {
        if (response.length < 5) {
            // throw IOException to mirror other protocol exceptions.
            throw new IOException("Invalid ack from a jetstream publish");
        }

        String s = new String(response, StandardCharsets.UTF_8);

        // check for error and then parse for speed.
        if (JetStreamApiResponse.isError(s)) {
            JetStreamApiResponse resp = new JetStreamApiResponse(response);
            if (resp.hasError()) {
                throw new IllegalStateException(resp.getError());
            }
        }

        Matcher m = streamRE.matcher(s);
        if (m.find()) {
            this.stream = m.group(1);
        }
        
        m = seqnoRE.matcher(s);
        if (m.find()) {
            this.seq = Long.parseLong(m.group(1));
        }

        m = duplicateRE.matcher(s);
        if (m.find()) {
            this.duplicate = Boolean.parseBoolean(m.group(1));
        }
    }

    @Override
    public long getSeqno() {
        return seq;
    }

    @Override
    public String getStream() {
        return stream;
    }

    @Override
    public boolean isDuplicate() {
        return duplicate;
    }
}
