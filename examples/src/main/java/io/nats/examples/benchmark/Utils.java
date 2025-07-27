// Copyright 2015-2018 The NATS Authors
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

// Copied from the old client for consistency

package io.nats.examples.benchmark;

import java.util.ArrayList;
import java.util.List;

public final class Utils {

    private Utils() {}

    /**
     * humanBytes formats bytes as a human readable string.
     *
     * @param bytes the number of bytes
     * @return a string representing the number of bytes in human readable string
     */
    public static String humanBytes(double bytes) {
        int base = 1024;
        String[] pre = new String[] {"k", "m", "g", "t", "p", "e"};
        String post = "b";
        if (bytes < (long) base) {
            return String.format("%.2f b", bytes);
        }
        int exp = (int) (Math.log(bytes) / Math.log(base));
        int index = exp - 1;
        String units = pre[index] + post;
        return String.format("%.2f %s", bytes / Math.pow((double) base, (double) exp), units);
    }

    /**
     * MsgsPerClient divides the number of messages by the number of clients and tries to distribute
     * them as evenly as possible.
     *
     * @param numMsgs    the total number of messages
     * @param numClients the total number of clients
     * @return an array of message counts
     */
    public static List<Integer> msgsPerClient(int numMsgs, int numClients) {
        List<Integer> counts = new ArrayList<Integer>(numClients);
        if (numClients == 0 || numMsgs == 0) {
            return counts;
        }
        int mc = numMsgs / numClients;
        for (int i = 0; i < numClients; i++) {
            counts.add(mc);
        }
        int extra = numMsgs % numClients;
        for (int i = 0; i < extra; i++) {
            counts.set(i, counts.get(i) + 1);
        }
        return counts;
    }
}
