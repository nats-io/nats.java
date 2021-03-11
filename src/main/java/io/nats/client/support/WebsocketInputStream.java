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

import java.io.IOException;
import java.io.InputStream;

public class WebsocketInputStream extends InputStream {
    private byte[] buffer = new byte[WebsocketFrameHeader.MAX_FRAME_HEADER_SIZE];
    private WebsocketFrameHeader header = new WebsocketFrameHeader();
    private InputStream in;
    private byte[] oneByte = new byte[1];

    public WebsocketInputStream(InputStream in) {
        this.in = in;
    }

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void mark(int readLimit) {

    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public int read(byte[] buffer) throws IOException {
        return read(buffer, 0, buffer.length);
    }

    @Override
    public int read(byte[] buffer, int offset, int length) throws IOException {
        // Just in case we get headers with empty payloads:
        while (0 == header.getPayloadLength()) {
            if (!readHeader()) {
                return -1;
            }
        }
        length = in.read(buffer, offset, length);
        if (-1 == length) {
            return length;
        }
        return header.filterPayload(buffer, offset, length);
    }

    @Override
    public int read() throws IOException {
        int result = read(oneByte, 0, 1);
        if (-1 == result) {
            return result;
        }
        return oneByte[0];
    }

    private boolean readHeader() throws IOException {
        int len = 0;
        while (len < 2) {
            int result = in.read(buffer, len, 2 - len);
            if (result < 0) {
                return false;
            }
            len += result;
        }
        int headerSize = WebsocketFrameHeader.size(buffer, 0);
        if (headerSize > 2) {
            while (len < headerSize) {
                int result = in.read(buffer, len, headerSize - len);
                if (result < 0) {
                    return false;
                }
                len += result;
            }
        }
        header.write(buffer, 0, headerSize);
        return true;
    }
}
