// Copyright 2015-2022 The NATS Authors
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

import io.nats.client.support.ByteArrayBuilder;

// ----------------------------------------------------------------------------------------------------
// Protocol message is a special version of a NatsPublishableMessage extends NatsMessage
// ----------------------------------------------------------------------------------------------------
class ProtocolMessage extends NatsPublishableMessage {

    ProtocolMessage(ByteArrayBuilder babProtocol) {
        super(false);
        protocolBab = babProtocol;
        sizeInBytes = controlLineLength = protocolBab.length() + 2; // CRLF, protocol doesn't have data
    }

    ProtocolMessage(byte[] protocol) {
        super(false);
        protocolBab = new ByteArrayBuilder(protocol);
        sizeInBytes = controlLineLength = protocolBab.length() + 2; // CRLF, protocol doesn't have data
    }

    ProtocolMessage(ProtocolMessage pm) {
        super(false);
        protocolBab = pm.protocolBab;
        sizeInBytes = controlLineLength = pm.sizeInBytes;
    }

    @Override
    boolean isProtocol() {
        return true;
    }

    @Override
    int copyNotEmptyHeaders(int destPosition, byte[] dest) {
        return 0; // until a protocol messages gets headers, might as well shortcut this.
    }
}
