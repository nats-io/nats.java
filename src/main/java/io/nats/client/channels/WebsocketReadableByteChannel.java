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
import java.nio.channels.ReadableByteChannel;
import java.util.function.Consumer;

import io.nats.client.support.WebsocketFrameHeader;
import io.nats.client.support.WebsocketFrameHeader.OpCode;

import static io.nats.client.support.BufferUtils.append;

/**
 * This class makes no attempt at thread safety. This is a lower-level
 * class, you probably want {@link WebsocketByteChannel} instead.
 * 
 * Wraps a ReadableByteChannel which is communicating via websockets:
 * 
 * https://datatracker.ietf.org/doc/html/rfc6455
 * 
 * NOTE: Websocket "frames" each have different OpCode(s) which should
 * be handled in different ways. For example, an OpCode.PING should be
 * handled by writing an OpCode.PONG websocket frame. Therefore the
 * ReadableByteChannel doesn't totally stand on its own and you are
 * better served by using the WebsocketByteChannel instead to handle
 * the full spec correctly. Similar issue with handling OpCode.CLOSE
 * properly.
 * 
 * Also note that the websocket handshake is NOT handled by this class.
 */
public class WebsocketReadableByteChannel implements ReadableByteChannel {
    private ReadableByteChannel wrap;
    private WebsocketFrameHeader header = new WebsocketFrameHeader()
        .withOp(OpCode.BINARY, true)
        .withNoMask();
    private ByteBuffer readBuffer;

    /**
     * @param wrap is the ReadableByteChannel to wrap.
     * @param readBuffer in "put" mode, contains any residual bytes from the handshake
     */
    public WebsocketReadableByteChannel(ReadableByteChannel wrap, ByteBuffer readBuffer) {
        this.wrap = wrap;
        this.readBuffer = readBuffer;
    }

    /**
     * Reads at most one websocket frame. Note that the frame may be empty in
     * which case the read will return 0. This channel does nothing special
     * if a close frame is read.
     * 
     * Note that the first read after the header may be a "short" read. This
     * is necessary to avoid an unintentional blocking read.
     * 
     * @param dst is the buffer in "put" mode which will be populated with the frame
     *     payload.
     * @return the number of bytes populated or -1 if the wrapped channel got closed.
     */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        return read(dst, null);
    }

    /**
     * If a websocket frame is read, op code consumer function will be called
     * with the new op code. No matter what, any data read is "put" into the
     * dst buffer.
     * 
     * @param dst is the buffer in "put" mode which will be populated with the frame
     *     payload.
     * @param opCodeConsumer is the function to call IF there is a new websocket frame.
     * @return the number of bytes populated or -1 if the wrapped channel got closed.
     * @throws IOException if input/output error occurs.
     */
    public int read(ByteBuffer dst, Consumer<OpCode> opCodeConsumer) throws IOException {
        // Read the header:
        if (0 == header.getPayloadLength()) {
            if (!readHeader()) {
                return -1;
            }
            if (null != opCodeConsumer) {
                opCodeConsumer.accept(header.getOpCode());
            }
        }

        int startPosition = dst.position();

        // Populate the payload:
        int length = header.getIntPayloadLength();
        length -= append(readBuffer, dst, length);
        if (dst.hasRemaining() && length > 0) {
            int saveLimit = dst.limit();
            if (dst.remaining() > length) {
                dst.limit(dst.position() + length);
            }
            try {
                wrap.read(dst);
            } finally {
                dst.limit(saveLimit);
            }
        }

        // Finally filter the payload that was populated:
        int totalLength = dst.position() - startPosition;
        header.filterPayload(dst, totalLength);

        return totalLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOpen() {
        return wrap.isOpen();
    }

    /**
     * Delegates to the wrapped channel. Note, that an orderly websocket close
     * requires a sequence defined in the RFC.
     */
    @Override
    public void close() throws IOException {
        wrap.close();
    }

    /**
     * Populates the internal buffer by reading from the wrapped
     * channel and then deserializing the websocket frame header.
     * 
     * @return false if the end of stream was observed before a header
     *     could be read, or wrapped read() call returned 0.
     * @throws IOException if input/output error occurs.
     */
    private boolean readHeader() throws IOException {
        int size = 2;
        while (readBuffer.position() < size) {
            if (readBuffer.capacity() != WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE) {
                ByteBuffer newBuffer = ByteBuffer.allocate(WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE);
                readBuffer.flip();
                newBuffer.put(readBuffer);
                readBuffer = newBuffer;
            }
            if (wrap.read(readBuffer) <= 0) {
                return false;
            }
            readBuffer.flip();
            try {
                size = WebsocketFrameHeader.size(readBuffer);
            } finally {
                readBuffer.compact();
            }
        }
        readBuffer.flip();
        try {
            header.deserialize(readBuffer);
        } finally {
            readBuffer.compact();
        }
        return true;
    }
}