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

package io.nats.client.channels;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.Random;

import io.nats.client.support.WebsocketFrameHeader;
import io.nats.client.support.WebsocketFrameHeader.OpCode;

import static io.nats.client.support.BufferUtils.remaining;

/**
 * This class makes no attempt at being thread safe. This is a low-level
 * class used to insert websocket frame headers, you probably want
 * {@link WebsocketByteChannel} instead.
 * 
 * Please consult the RFC for details: https://datatracker.ietf.org/doc/html/rfc6455
 */
public class WebsocketWritableByteChannel implements GatheringByteChannel {
    private final GatheringByteChannel wrap;
    private final ByteBuffer buffer = ByteBuffer.allocate(WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE);
    private final WebsocketFrameHeader header = new WebsocketFrameHeader();
    private final Random random;
    private final boolean masked;
    private final OpCode defaultOpCode;

    /**
     * @param wrap is the channel to wrap
     * @param masked should be set to true if websocket "masking" should be enabled.
     * @param opCode is the default opCode that should be used for write() calls,
     *    probably only makes sense to use OpCode.BINARY or OpCode.TEXT.
     * @param random is a random number generator to use when masking.
     */
    public WebsocketWritableByteChannel(GatheringByteChannel wrap, boolean masked, OpCode opCode, Random random) {
        this.wrap = wrap;
        this.masked = masked;
        this.random = random;
        this.defaultOpCode = opCode;
    }

    /**
     * @return true if the wrapped channel is open
     */
    @Override
    public boolean isOpen() {
        return wrap.isOpen();
    }

    /**
     * Closes the wrapped channel. Note, that per the websocket spec, a close OpCode
     * should be sent with a status code + message body, and then wait to recieve
     * acknowledgement. Higher level code is responsible for that sequence.
     */
    @Override
    public void close() throws IOException {
        wrap.close();
    }

    /**
     * Peforms a write of a new websocket frame with the specified op code and
     * finalFragment fields for a list of src buffers starting at the specified offset
     * and continuing for the specified length.
     * 
     * @param srcs are an array of buffers in "get" mode to write. This may be an empty
     *     list if the websocket frame payload is to be empty. Note that original bytes in
     *     the buffer may not be preserved if masking is enabled.
     * @param offset is the offset within srcs to begin writing.
     * @param length is the total number of byte buffers within the srcs to write.
     * @param code is the OpCode to use for the header.
     * @param isFinalFragment should only be false if fragmenting a message into
     *     multiple frames and this is NOT the final fragment.
     * @return the number of bytes written from src, which will always be the
     *    remaining size of the source unless the end of stream was observed.
     * @throws IOException if input/output exception occured
     */
    public long write(ByteBuffer[] srcs, int offset, int length, OpCode code, boolean isFinalFragment) throws IOException {
        long payloadLength = remaining(srcs, offset, length);

        // Populate the header:
        header.withPayloadLength(payloadLength)
            .withOp(code, isFinalFragment);
        if (masked) {
            header.withMask(random.nextInt());
        }
        buffer.clear();
        try {
            header.serialize(buffer);
        } finally {
            buffer.flip();
        }

        // Mask the srcs:
        int end = offset + length;
        for (int i = offset; i < end; i++) {
            int savePosition = srcs[i].position();
            srcs[i].position(srcs[i].limit());
            header.filterPayload(srcs[i], srcs[i].position() - savePosition);
            srcs[i].position(savePosition);
        }

        // Prepend the header buffer:
        ByteBuffer[] srcsWithHeader = new ByteBuffer[length + 1];
        srcsWithHeader[0] = buffer;
        for (int i = offset; i < end; i++) {
            srcsWithHeader[i + 1] = srcs[i];
        }

        long result = wrap.write(srcsWithHeader);

        // Conceal the header size:
        if (result < 0) {
            return result;
        } else if (result < header.size()) {
            return result;
        } else {
            return result - header.size();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int write(ByteBuffer src) throws IOException {
        return (int)write(new ByteBuffer[]{src}, 0, 1, defaultOpCode, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return write(srcs, offset, length, defaultOpCode, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return write(srcs, 0, srcs.length);
    }
}