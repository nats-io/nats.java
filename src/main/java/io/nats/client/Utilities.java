/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

class Utilities {

    /**
     * Convert a string containing only ASCII characters to the equivalent byte array.
     * 
     * @param str the string to convert.
     * @return a byte array containing the character bytes of of each character composing the input
     *         string.
     */
    public static byte[] stringToBytesASCII(String str) {
        byte[] b = new byte[str.length()];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) str.charAt(i);
        }
        return b;
    }


    /**
     * Copy an ASCII string to the specified offset in a byte array.
     * 
     * This method assumes that the input string value is ASCII (single byte) encoding. Please do
     * not arbitrarily change this method without doing your research. It is currently the fastest
     * way to perform this operation as of Java 1.6.
     * 
     * @param buffer the destination byte array
     * @param offset the buffer offset to write the string's bytes to.
     * @param str the string to copy
     *
     */
    public static int stringToBytesASCII(byte[] buffer, int offset, String str) {
        int length = str.length();
        int end = offset + length;
        for (int i = 0; i < length; i++) {
            buffer[i + offset] = (byte) str.charAt(i);
        }
        return end;
    }
}
