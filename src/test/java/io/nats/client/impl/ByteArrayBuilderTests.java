package io.nats.client.impl;

import org.junit.jupiter.api.Test;

import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.nats.client.support.NatsConstants.OP_PING_BYTES;
import static io.nats.client.support.NatsConstants.OP_PONG_BYTES;
import static io.nats.client.support.RandomUtils.PRAND;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static org.junit.jupiter.api.Assertions.*;

public class ByteArrayBuilderTests {

    @Test
    public void byte_array_builder_works() {
        ByteArrayBuilder bab = new ByteArrayBuilder();
        String testString = "abcdefghij";
        _test(PRAND, bab, Collections.singletonList(testString), StandardCharsets.US_ASCII);

        List<String> subjects = dataAsLines("utf8-test-strings.txt");
        bab = new ByteArrayBuilder(StandardCharsets.UTF_8);
        _test(PRAND, bab, subjects, StandardCharsets.UTF_8);
    }

    @Test
    public void equalsBytes() {
        ByteArrayBuilder bab = new ByteArrayBuilder();
        bab.append(OP_PING_BYTES);
        assertTrue(bab.equals(OP_PING_BYTES));
        assertFalse(bab.equals(OP_PONG_BYTES));
    }

    @Test
    public void copyTo() {
        ByteArrayBuilder bab = new ByteArrayBuilder();
        bab.append("0123456789");
        byte[] target = "AAAAAAAAAAAAAAAAAAAA".getBytes(StandardCharsets.US_ASCII);
        assertEquals(10, bab.copyTo(target, 0));
        assertEquals(10, bab.copyTo(target, 10));
        assertEquals("01234567890123456789", new String(target));
    }


    @Test
    public void coverage() {
        ByteArrayBuilder bab = new ByteArrayBuilder(1)
                .appendSpace()
                .appendCrLf()
                .append((String)null)
                .append("foo")
                .append((CharBuffer)null)
                .append(CharBuffer.wrap("bar"))
                .append(4273)
                .append(new byte[0])
                .append(new byte[0], 0)
                .append("baz".getBytes())
                .append(new byte[0], 0)
                .append("baz".getBytes(), 3)
                .append("baz".getBytes(), 2, 1)
                ;
        assertEquals(" \r\nnullfoonullbar4273bazbazz", bab.toString());
    }

    private void _test(Random r, ByteArrayBuilder bab, List<String> testStrings, Charset charset) {
        String expectedString = "";
        for (String testString : testStrings) {
            int loops = 1000 / testString.length();
            for (int x = 1; x < loops; x++) {
                String more = testString + r.nextInt(Integer.MAX_VALUE);
                bab.append(more);
                expectedString = expectedString + more;
                byte[] bytes = bab.toByteArray();
                assertEquals(expectedString.getBytes(charset).length, bytes.length);
                assertEquals(expectedString, new String(bytes, charset));
            }
        }
    }
}
