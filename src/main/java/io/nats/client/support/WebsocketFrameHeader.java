// Copyright 2021 The NATS Authors
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

public class WebsocketFrameHeader {
    public static int MAX_FRAME_HEADER_SIZE = 14;

    public enum OpCode {
        CONTINUATION(0),
        TEXT(1),
        BINARY(2),
        CLOSE(8),
        PING(9),
        PONG(10),
        UNKNOWN(0x10);

        private int code;

        OpCode(int code) {
            this.code = code;
        }

        public int getCode() {
            return this.code;
        }

        public static OpCode of(int code) {
            switch (code) {
                case 0: return CONTINUATION;
                case 1: return TEXT;
                case 2: return BINARY;
                case 8: return CLOSE;
                case 9: return PING;
                case 10: return PONG;
            }
            return UNKNOWN;
        }
    }

    // Fields of header:
    private byte byte0;
    private boolean mask;
    private long payloadLength;
    private int maskingKey;
    private int maskingKeyOffset = 0;

    public WebsocketFrameHeader withOp(OpCode op, boolean isFinal) {
        this.byte0 = (byte)(op.getCode() | (isFinal ? 0x80 : 0));
        return this;
    }

    public WebsocketFrameHeader withNoMask() {
        this.mask = false;
        return this;
    }

    public WebsocketFrameHeader withMask(int maskingKey) {
        this.mask = true;
        this.maskingKey = maskingKey;
        this.maskingKeyOffset = 0;
        return this;
    }

    public WebsocketFrameHeader withPayloadLength(long payloadLength) {
        this.payloadLength = payloadLength;
        return this;
    }

    public boolean isFinal() {
        return (byte0 & 0x80) != 0;
    }

    public boolean isMasked() {
        return mask;
    }

    public int getMaskingKey() {
        return maskingKey;
    }

    public long getPayloadLength() {
        return payloadLength;
    }

    public int getIntPayloadLength() {
        if (payloadLength > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int)payloadLength;
    }

    public OpCode getOpCode() {
        return OpCode.of(byte0 & 0xF);
    }

    public boolean isPayloadEmpty() {
        return 0 == payloadLength;
    }

    /**
     * Filters the payload BEFORE position of size specified by the length.
     * Note that length must NOT exceed payloadLength, and there MUST be
     * at least length bytes BEFORE position (aka length &lt;= position)
     *
     * @param buffer in "put" mode, position and limit will NOT be changed,
     *    but the bytes between <code>[position - length, position)</code>
     *    may be modified if {@link #isMasked()}.
     * @param length is the number of bytes BEFORE the position which
     *    should be filtered, this MUST be &lt;= {@link #getIntPayloadLength()}
     * NOTE: payloadLength and maskingKeyOffset will be altered as a
     * side effect, so this method MUST be called even if masking is
     * disabled to decrement the payloadLength.
     */
    public void filterPayload(ByteBuffer buffer, int length) {
        // Preconditions:
        assert length <= getIntPayloadLength();
        assert buffer.position() >= length;

        payloadLength -= length;
        if (mask && length > 0) {
            int finalPosition = buffer.position();
            buffer.position(buffer.position() - length);
            for (int i=buffer.position(); i < finalPosition; i++) {
                int key = 0xFF & (maskingKey >> (8 * (7 - maskingKeyOffset)));
                buffer.put((byte)(buffer.get(i) ^ key));
                maskingKeyOffset = (maskingKeyOffset + 1) % 8;
            }
        }
    }

    /**
     * @return the size of this websocket frame header.
     */
    public int size() {
        int size = 2;
        if (payloadLength > 0xFFFF) {
            size += 8;
        } else if (payloadLength > 125) {
            size += 2;
        }
        if (mask) {
            size += 4;
        }
        return size;
    }

    /**
     * Reads bytes at position and position + 1 to determine the
     * total size of the header.
     *
     * @param buffer in "get" mode, no state changes is made
     * @return total size of the websocket header, or 0 if precondition
     *     is invalid.
     *
     * Precondition: buffer remaining() &gt;= 2
     */
    public static int size(ByteBuffer buffer) {
        if (buffer.remaining() < 2) {
            return 0;
        }

        int size = 2;
        byte byte1 = buffer.get(buffer.position() + 1);
        if (0 != (byte1 & 0x80)) {
            // mask adds 4 required bytes.
            size += 4;
        }

        switch (byte1 & 0x7F) {
        case 126:
            size += 2;
            break;
        case 127:
            size += 8;
            break;
        }
        return size;
    }

    /**
     * Serializes this WebsocketFrameHeader into buffer if sufficient space
     * is available in the buffer, otherwise returns 0.
     * 
     * @param buffer in "put" mode where the serialized header will be placed,
     *    must have a buffer.remaining() &gt;= this.size(), otherwise returns 0.
     * 
     * @return 0 if there is insufficient remainder in the buffer, otherwise
     *    returns the number of bytes placed into the buffer.
     */
    public int serialize(ByteBuffer buffer) {
        if (buffer.remaining() < size()) {
            return 0;
        }

        int startPosition = buffer.position();
        buffer.put(byte0);
        if (payloadLength > 0xFFFF) {
            buffer.put((byte)(127 | (mask ? 0x80 : 0)));
            // 64 bit length
            buffer.put((byte)((payloadLength >> 56) & 0xFF));
            buffer.put((byte)((payloadLength >> 48) & 0xFF));
            buffer.put((byte)((payloadLength >> 40) & 0xFF));
            buffer.put((byte)((payloadLength >> 32) & 0xFF));
            buffer.put((byte)((payloadLength >> 24) & 0xFF));
            buffer.put((byte)((payloadLength >> 16) & 0xFF));
            buffer.put((byte)((payloadLength >> 8) & 0xFF));
            buffer.put((byte)(payloadLength & 0xFF));
        } else if (payloadLength > 125) {
            buffer.put((byte)(126 | (mask ? 0x80 : 0)));
            // 16 bit length
            buffer.put((byte)(payloadLength >> 8));
            buffer.put((byte)(payloadLength & 0xFF));
        } else {
            buffer.put((byte)(payloadLength | (mask ? 0x80 : 0)));
        }
        if (mask) {
            buffer.put((byte)((maskingKey >> 24) & 0xFF));
            buffer.put((byte)((maskingKey >> 16) & 0xFF));
            buffer.put((byte)((maskingKey >> 8) & 0xFF));
            buffer.put((byte)(maskingKey & 0xFF));
        }
        return buffer.position() - startPosition;
    }

    /**
     * Deserializes the websocket header contained in the buffer into this
     * WebsocketFrameHeader instance. We do this so a single WebsocketFrameHeader
     * instance can be reused rather than allocating a new one each time.
     * 
     * <pre>
     * buffer = ByteBuffer.allocate(WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE);
     * reader.read(buffer);
     * buffer.flip(); // Place in "get" mode
     * WebsocketFrameHeader header = new WebsocketFrameHeader()
     * header.deserialize(buffer);
     * </pre>
     * 
     * @param buffer in "get" mode containing the serialized header.
     * 
     * @return the number of bytes removed from buffer, or 0 if precondition
     *    is not met.
     * 
     * Precondition: buffer has remaining bytes which are &gt;= the header
     *    {@link #size(ByteBuffer)}.
     * Postcondition: this header instance is populated with the websocket
     *    header deserialized from the buffer.
     */
    public int deserialize(ByteBuffer buffer) {
        if (buffer.remaining() < 2 ||
            buffer.remaining() < size(buffer))
        {
            return 0;
        }

        int startPosition = buffer.position();
        byte0 = buffer.get();
        byte byte1 = buffer.get();
        mask = 0 != (byte1 & 0x80);
        payloadLength = byte1 & 0x7F;
        if (126 == payloadLength) {
            payloadLength = 0;
            for (int i=0; i < 2; i++) {
                payloadLength <<= 8;
                payloadLength |= buffer.get() & 0xFF;
            }
        } else if (127 == payloadLength) {
            payloadLength = 0;
            for (int i=0; i < 8; i++) {
                payloadLength <<= 8;
                payloadLength |= buffer.get() & 0xFF;
            }
        }
        if (mask) {
            maskingKey = 0;
            maskingKeyOffset = 0;
            for (int i=0; i < 4; i++) {
                maskingKey <<= 8;
                maskingKey |= buffer.get() & 0xFF;
            }
        }
        return buffer.position() - startPosition;
    }
}
