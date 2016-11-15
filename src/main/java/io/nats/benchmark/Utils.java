/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.benchmark;

import java.util.ArrayList;
import java.util.List;

public final class Utils {

    private Utils() {}

    /**
     * humanBytes formats bytes as a human readable string.
     *
     * @param bytes the number of bytes
     * @param si    whether to use SI units
     * @return a string representing the number of bytes in human readable string
     */
    public static String humanBytes(double bytes, boolean si) {
        int base = 1024;
        String[] pre = new String[] {"K", "M", "G", "T", "P", "E"};
        String post = "B";
        if (si) {
            base = 1000;
            pre = new String[] {"k", "M", "G", "T", "P", "E"};
            post = "iB";
        }
        if (bytes < (long) base) {
            return String.format("%.2f B", bytes);
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
