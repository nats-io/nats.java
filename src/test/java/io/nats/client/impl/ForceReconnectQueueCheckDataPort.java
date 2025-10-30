// Copyright 2024 The NATS Authors
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

public class ForceReconnectQueueCheckDataPort extends SocketDataPort {
    private static byte[] WRITE_CHECK;
    private static int WC_LEN;
    public static long DELAY;

    public static void setCheck(String check) {
        WRITE_CHECK = check.getBytes(StandardCharsets.ISO_8859_1);
        WC_LEN = check.length();
    }

    @Override
    public void write(byte[] src, int toWrite) throws IOException {
        if (src.length >= WC_LEN) {
            boolean check = true;
            for (int x = 0; x < WC_LEN; x++) {
                if (src[x] != WRITE_CHECK[x]) {
                    check = false;
                    break;
                }
            }
            if (check) {
                try {
                    Thread.sleep(DELAY);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        super.write(src, toWrite);
    }
}
