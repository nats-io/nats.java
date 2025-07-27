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

public class ForceReconnectQueueCheckDataPort extends SocketDataPort {
    public static String WRITE_CHECK;
    public static long DELAY;

    @Override
    public void write(byte[] src, int toWrite) throws IOException {
        String s = new String(src, 0, Math.min(7, toWrite));
        if (s.startsWith(WRITE_CHECK)) {
            try {
                Thread.sleep(DELAY);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        super.write(src, toWrite);
    }
}
