package io.nats.client.impl;

import java.nio.CharBuffer;

public class AuthHandlerUtil {

    /**
     * Extracts a char array from the given CharBuffer based on header information.
     *
     * @param data The CharBuffer containing the data to extract from.
     * @param headers The number of headers to account for in the extraction.
     * @return A char array containing the extracted data.
     */
    public static char[] extract(CharBuffer data, int headers) {
        CharBuffer buff = CharBuffer.allocate(data.length());
        boolean skipLine = false;
        int headerCount = 0;
        int linePos = -1;

        while (data.length() > 0) {
            char c = data.get();
            linePos++;

            // End of line, either we got it, or we should keep reading the new line
            if (c == '\n' || c=='\r') {
                if (buff.position() > 0) { // we wrote something
                    break;
                }
                skipLine = false;
                linePos = -1; // so we can start right up
                continue;
            }

            // skip to the new line
            if (skipLine) {
                continue;
            }

            // Ignore whitespace
            if (Character.isWhitespace(c)) {
                continue;
            }

            // If we are on a - skip that line, bump the header count
            if (c == '-' && linePos == 0) {
                skipLine = true;
                headerCount++;
                continue;
            }

            // Skip the line, or add to buff
            if (headerCount==headers) {
                buff.put(c);
            }
        }

        // check for naked value
        if (buff.position() == 0 && headers==1) {
            data.position(0);
            while (data.length() > 0) {
                char c = data.get();
                if (c == '\n' || c=='\r' || Character.isWhitespace(c)) {
                    if (buff.position() > 0) { // we wrote something
                        break;
                    }
                    continue;
                }

                buff.put(c);
            }
            buff.flip();
        } else {
            buff.flip();
        }

        char[] retVal = new char[buff.length()];
        buff.get(retVal);
        buff.clear();
        for (int i=0; i<buff.capacity();i++) {
            buff.put('\0');
        }
        return retVal;
    }

}

