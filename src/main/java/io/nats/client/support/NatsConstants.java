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

package io.nats.client.support;

import java.nio.ByteBuffer;

public interface NatsConstants {
    ByteBuffer VERSION = ByteBuffer.wrap(new byte[] { 'N', 'A', 'T', 'S', '/' , '1', '.', '0'});
    String SPACE = " ";
    String EMPTY = "";
    ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
    ByteBuffer CRLF = ByteBuffer.wrap(new byte[] { '\r', '\n'});

    byte TAB = '\t';
    byte SP = ' ';
    byte COLON = ':';
    byte CR = '\r';
    byte LF = '\n';

    byte[] EMPTY_BODY = new byte[0];
    ByteBuffer EMPTY_BODY_BUFFER = ByteBuffer.allocate(0);
    byte[] VERSION_BYTES = VERSION.array();
    ByteBuffer VERSION_BYTES_PLUS_CRLF = ByteBuffer.wrap(new byte[] { 'N', 'A', 'T', 'S', '/' , '1', '.', '0', '\r', '\n'});
    byte[] COLON_BYTES = new byte[] { ':' };
    byte[] CRLF_BYTES = CRLF.array();
    int CRLF_BYTES_LEN = CRLF_BYTES.length;
    int VERSION_BYTES_LEN = VERSION_BYTES.length;
    int VERSION_BYTES_PLUS_CRLF_LEN = VERSION_BYTES_PLUS_CRLF.limit();

    String OP_CONNECT = "CONNECT";
    String OP_INFO = "INFO";
    String OP_SUB = "SUB";
    String OP_PUB = "PUB";
    String OP_HPUB = "HPUB";
    String OP_UNSUB = "UNSUB";
    String OP_MSG = "MSG";
    String OP_HMSG = "HMSG";
    String OP_PING = "PING";
    String OP_PONG = "PONG";
    String OP_OK = "+OK";
    String OP_ERR = "-ERR";
    String UNKNOWN_OP = "UNKNOWN";

    ByteBuffer OP_PING_BYTES = ByteBuffer.wrap(new byte[] { 'P', 'I', 'N', 'G' });
    ByteBuffer OP_PONG_BYTES = ByteBuffer.wrap(new byte[] { 'P', 'O', 'N', 'G' });

    ByteBuffer PUB_SP_BYTES = ByteBuffer.wrap(new byte[] { 'P', 'U', 'B', ' ' });
    ByteBuffer HPUB_SP_BYTES = ByteBuffer.wrap(new byte[] { 'H', 'P', 'U', 'B', ' ' });
    ByteBuffer CONNECT_SP_BYTES = ByteBuffer.wrap(new byte[] { 'C', 'O', 'N', 'N', 'E', 'C', 'T', ' ' });
    ByteBuffer SUB_SP_BYTES = ByteBuffer.wrap(new byte[] { 'S', 'U', 'B', ' ' });
    ByteBuffer UNSUB_SP_BYTES = ByteBuffer.wrap(new byte[] { 'U', 'N', 'S', 'U', 'B' , ' '});

    int OP_PUB_SP_LEN = PUB_SP_BYTES .limit();
    int OP_HPUB_SP_LEN = HPUB_SP_BYTES.limit();
    int OP_CONNECT_SP_LEN = CONNECT_SP_BYTES.limit();
    int OP_SUB_SP_LEN = SUB_SP_BYTES.limit();
    int OP_UNSUB_SP_LEN = UNSUB_SP_BYTES.limit();

    int MAX_PROTOCOL_RECEIVE_OP_LENGTH = 4;

    String INVALID_HEADER_VERSION = "Invalid header version";
    String INVALID_HEADER_COMPOSITION = "Invalid header composition";
    String INVALID_HEADER_STATUS_CODE = "Invalid header status code";
    String SERIALIZED_HEADER_CANNOT_BE_NULL_OR_EMPTY = "Serialized header cannot be null or empty.";

    int STATUS_NO_RESPONDER = 503;
}
