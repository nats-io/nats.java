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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.nats.client.PublishAck;

// Internal class to handle jetstream acknowedgements
class PublishAckImpl implements PublishAck {

    private String stream = null;
    private long seq = -1;

    private static final String grabString = "\\s*\"(.+?)\"";
    private static final String grabNumber = "\\s*(\\d+)";

    private static final Pattern streamRE = Pattern.compile("\"stream\":" + grabString, Pattern.CASE_INSENSITIVE);
    private static final Pattern seqnoRE = Pattern.compile("\"seq\":" + grabNumber, Pattern.CASE_INSENSITIVE); 

    // Acks will be received with the following format:
    // "-ERR <server message>""
    // "+OK { "stream" : "mystream", "seq", 42}"
    public PublishAckImpl(byte[] response) throws IOException {
        // "-ERR Server Message"
        if (response.length < 5) {
            // throw IOException to mirror other protocol exceptions.
            throw new IOException("Invalid ack from a jetstream publish");
        }

        String s = new String(response, StandardCharsets.UTF_8);
        if (s.startsWith("-ERR")) {
            throw new IOException(s.substring(5));
        }

        if (!s.startsWith("+OK")) {
            throw new IOException("Invalid protocol message: " + s);
        }
        
        Matcher m = streamRE.matcher(s);
        if (m.find()) {
            this.stream = m.group(1);
        }
        
        m = seqnoRE.matcher(s);
        if (m.find()) {
            this.seq = Long.parseLong(m.group(1));
        }
    }

    public long getSeqno() {
        return seq;
    }

    public String getStream() {
        return stream;
    }
}
