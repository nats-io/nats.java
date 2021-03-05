package io.nats.client.support;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Random;

public abstract class RandomUtils {
    private RandomUtils() {}  /* ensures cannot be constructed */

    public static final SecureRandom SRAND = new SecureRandom();
    public static final Random PRAND = new Random(bytesToLong(SRAND.generateSeed(8))); // seed with 8 bytes (64 bits)

    public static long nextLong(Random rng, long maxValue) {
        // error checking and 2^x checking removed for simplicity.
        long bits;
        long val;
        do {
            bits = (rng.nextLong() << 1) >>> 1;
            val = bits % maxValue;
        } while (bits - val + (maxValue - 1) < 0L);
        return val;
    }

    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.put(bytes);
        buffer.flip();// need flip
        return buffer.getLong();
    }

}
