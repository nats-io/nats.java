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

    public OpCode getOpCode() {
        return OpCode.of(byte0 & 0xF);
    }

    public boolean isPayloadEmpty() {
        return 0 == payloadLength;
    }

    /**
     * Decrement the payloadLength by at most maxSize such that payloadLength is non-negative,
     * returning the amount decremented.
     * 
     * @param buffer is the buffer to filter.
     * @param offset is the start offset within buffer to filter.
     * @param length is the number of bytes to filter.
     * 
     * @return min(payloadLength, maxSize), decrementing the internal payloadLength by this amount.
     */
    public int filterPayload(byte[] buffer, int offset, int length) {
        length = Math.min(length, payloadLength > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)payloadLength);
        payloadLength -= length;
        if (mask) {
            for (int i=0; i < length; i++) {
                int key = 0xFF & (maskingKey >> (8 * (7 - maskingKeyOffset)));
                buffer[offset + i] ^= key;
                maskingKeyOffset = (maskingKeyOffset + 1) % 8;
            }
        }
        return length;
    }

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
     * Introspects the first 2 bytes of buffer at the specified offset to
     * determine how large the entire header is.
     * 
     * @param buffer is the buffer to introspect
     * @param offset is the offset within the buffer where the websocket
     *     header begins.
     * @return the number of bytes used by the full websocket header.
     */
    public static int size(byte[] buffer, int offset) {
        int size = 2;
        if (0 != (buffer[offset + 1] & 0x80)) {
            // mask adds 4 required bytes.
            size += 4;
        }

        switch (buffer[offset + 1] & 0x7F) {
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
     * Serializes this WebsocketFrameHeader into a buffer.
     * 
     * @param buffer where the serialized header will be placed.
     * 
     * @param offset is the start offset into the buffer.
     * 
     * @param length is the max bytes that can be placed in the buffer.
     * 
     * @return 0 if there is insufficient remainder in the buffer, otherwise
     *    returns the number of bytes placed into the buffer.
     */
    public int read(byte[] buffer, int offset, int length) {
        if (length < size()) {
            return 0;
        }

        int startOffset = offset;

        buffer[offset++] = byte0;
        if (payloadLength > 0xFFFF) {
            buffer[offset++] = (byte)(127 | (mask ? 0x80 : 0));
            // 64 bit length
            buffer[offset++] = (byte)((payloadLength >> 56) & 0xFF);
            buffer[offset++] = (byte)((payloadLength >> 48) & 0xFF);
            buffer[offset++] = (byte)((payloadLength >> 40) & 0xFF);
            buffer[offset++] = (byte)((payloadLength >> 32) & 0xFF);
            buffer[offset++] = (byte)((payloadLength >> 24) & 0xFF);
            buffer[offset++] = (byte)((payloadLength >> 16) & 0xFF);
            buffer[offset++] = (byte)((payloadLength >> 8) & 0xFF);
            buffer[offset++] = (byte)(payloadLength & 0xFF);
        } else if (payloadLength > 125) {
            buffer[offset++] = (byte)(126 | (mask ? 0x80 : 0));
            // 16 bit length
            buffer[offset++] = (byte)(payloadLength >> 8);
            buffer[offset++] = (byte)(payloadLength & 0xFF);
        } else {
            buffer[offset++] = (byte)(payloadLength | (mask ? 0x80 : 0));
        }
        if (mask) {
            buffer[offset++] = (byte)((maskingKey >> 24) & 0xFF);
            buffer[offset++] = (byte)((maskingKey >> 16) & 0xFF);
            buffer[offset++] = (byte)((maskingKey >> 8) & 0xFF);
            buffer[offset++] = (byte)(maskingKey & 0xFF);
        }
        return offset - startOffset;
    }

    /**
     * Overwrite internal frame header content with input buffer.
     * 
     * <pre>
     * WebsocketFrameHeader header = new WebsocketFrameHeader();
     * int consumedLength = header.write(buffer, offset, length);
     * offset += consumedLength;
     * length -= consumedLength;
     * </pre>
     * 
     * @param buffer containing the serialized header.
     * 
     * @param offset is the start offset into the buffer.
     * 
     * @param length is the max bytes to consume.
     * 
     * @return 0 if there is insufficient remainder in the buffer, otherwise
     *     returns the number of bytes consumed from the buffer.
     */
    public int write(byte[] buffer, int offset, int length) {
        // Sufficient remainder?
        if (length < 2) {
            return 0;
        }
        int size = size(buffer, offset);
        if (size > length) {
            return 0;
        }

        byte0 = buffer[offset++];
        mask = 0 != (buffer[offset] & 0x80);
        payloadLength = buffer[offset] & 0x7F;
        offset++;
        if (126 == payloadLength) {
            payloadLength = 0;
            for (int i=0; i < 2; i++) {
                payloadLength <<= 8;
                payloadLength |= buffer[offset++] & 0xFF;
            }
        } else if (127 == payloadLength) {
            payloadLength = 0;
            for (int i=0; i < 8; i++) {
                payloadLength <<= 8;
                payloadLength |= buffer[offset++] & 0xFF;
            }
        }
        if (mask) {
            maskingKey = 0;
            maskingKeyOffset = 0;
            for (int i=0; i < 4; i++) {
                maskingKey <<= 8;
                maskingKey |= buffer[offset++] & 0xFF;
            }
        }
        return size;
    }
}
