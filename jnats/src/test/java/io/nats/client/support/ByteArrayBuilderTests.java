package io.nats.client.support;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static io.nats.client.support.BuilderBase.ALLOCATION_BOUNDARY;
import static io.nats.client.support.ByteArrayBuilder.DEFAULT_ASCII_ALLOCATION;
import static io.nats.client.support.ByteArrayBuilder.DEFAULT_OTHER_ALLOCATION;
import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.support.RandomUtils.PRAND;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

public class ByteArrayBuilderTests {

    @Test
    public void byte_array_builder_works() {
        List<String> testStrings = Collections.singletonList("abcdefghij");
        _works(new ByteArrayBuilder(), testStrings, US_ASCII);
        _works(new ByteArrayPrimitiveBuilder(), testStrings, US_ASCII);

        testStrings = dataAsLines("utf8-test-strings.txt");
        _works(new ByteArrayBuilder(UTF_8), testStrings, UTF_8);
        _works(new ByteArrayPrimitiveBuilder(UTF_8), testStrings, UTF_8);
    }

    private void _works(ByteArrayBuilder bab, List<String> testStrings, Charset charset) {
        String expectedString = "";
        for (String testString : testStrings) {
            int loops = 1000 / testString.length();
            for (int x = 1; x < loops; x++) {
                String more = testString + PRAND.nextInt(Integer.MAX_VALUE);
                bab.append(more);
                expectedString = expectedString + more;
                byte[] bytes = bab.toByteArray();
                assertEquals(expectedString.getBytes(charset).length, bytes.length);
                assertEquals(expectedString, new String(bytes, charset));
            }
        }
    }

    private void _works(ByteArrayPrimitiveBuilder bab, List<String> testStrings, Charset charset) {
        String expectedString = "";
        for (String testString : testStrings) {
            int loops = 1000 / testString.length();
            for (int x = 1; x < loops; x++) {
                String more = testString + PRAND.nextInt(Integer.MAX_VALUE);
                bab.append(more);
                expectedString = expectedString + more;
                byte[] bytes = bab.toByteArray();
                assertEquals(expectedString.getBytes(charset).length, bytes.length);
                assertEquals(expectedString, new String(bytes, charset));
            }
        }
    }

    @Test
    public void equalsBytes() {
        _equalsBytes(new ByteArrayBuilder(OP_PING_BYTES));
        _equalsBytes(new ByteArrayPrimitiveBuilder(OP_PING_BYTES));
    }

    private static void _equalsBytes(ByteArrayBuilder bab) {
        assertTrue(bab.equals(OP_PING_BYTES));
        assertFalse(bab.equals(OP_PONG_BYTES));
        assertFalse(bab.equals(null));
        assertFalse(bab.equals("x".getBytes()));
    }

    private static void _equalsBytes(ByteArrayPrimitiveBuilder bab) {
        assertTrue(bab.equals(OP_PING_BYTES));
        assertFalse(bab.equals(OP_PONG_BYTES));
        assertFalse(bab.equals(null));
        assertFalse(bab.equals("x".getBytes()));
    }

    @Test
    public void copyTo() throws IOException {
        ByteArrayBuilder bab = new ByteArrayBuilder();
        bab.append("0123456789");
        byte[] target = "AAAAAAAAAAAAAAAAAAAA".getBytes(US_ASCII);
        assertEquals(10, bab.copyTo(target, 0));
        assertEquals(10, bab.copyTo(target, 10));
        assertEquals("01234567890123456789", new String(target));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        bab.copyTo(out);
        assertEquals(10, out.toString().length());
    }

    @Test
    public void copyToPrimitiveBuilder() throws IOException {
        ByteArrayPrimitiveBuilder bab = new ByteArrayPrimitiveBuilder();
        bab.append("0123456789");
        byte[] target = "AAAAAAAAAAAAAAAAAAAA".getBytes(US_ASCII);
        assertEquals(10, bab.copyTo(target, 0));
        assertEquals(10, bab.copyTo(target, 10));
        assertEquals("01234567890123456789", new String(target));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        bab.copyTo(out);
        assertEquals(10, out.toString().length());
    }

    @Test
    public void constructorCoverage() {
        ByteArrayBuilder bab = new ByteArrayBuilder("0123456789".getBytes());
        assertEquals("0123456789", bab.toString());
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(10, bab.length());

        bab = new ByteArrayBuilder(-1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, -1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, -1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, DEFAULT_ASCII_ALLOCATION - 1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, DEFAULT_OTHER_ALLOCATION - 1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, DEFAULT_ASCII_ALLOCATION + 1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, DEFAULT_OTHER_ALLOCATION + 1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION + ALLOCATION_BOUNDARY, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION + ALLOCATION_BOUNDARY, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayBuilder(-1, DEFAULT_OTHER_ALLOCATION + ALLOCATION_BOUNDARY + 1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION * 2, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION * 2, bab.getAllocationSize());
        assertEquals(0, bab.length());
    }

    @Test
    public void constructorCoverageByteArrayPrimitiveBuilder() {
        ByteArrayPrimitiveBuilder bab = new ByteArrayPrimitiveBuilder("0123456789".getBytes());
        assertEquals("0123456789", bab.toString());
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(10, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, -1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, -1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, DEFAULT_ASCII_ALLOCATION - 1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, DEFAULT_OTHER_ALLOCATION - 1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, DEFAULT_ASCII_ALLOCATION + 1, US_ASCII);
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.internalArray().length);
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, DEFAULT_OTHER_ALLOCATION + 1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION + ALLOCATION_BOUNDARY, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION + ALLOCATION_BOUNDARY, bab.getAllocationSize());
        assertEquals(0, bab.length());

        bab = new ByteArrayPrimitiveBuilder(-1, DEFAULT_OTHER_ALLOCATION + ALLOCATION_BOUNDARY + 1, UTF_8);
        assertEquals(DEFAULT_OTHER_ALLOCATION * 2, bab.internalArray().length);
        assertEquals(DEFAULT_OTHER_ALLOCATION * 2, bab.getAllocationSize());
        assertEquals(0, bab.length());
    }

    @Test
    public void miscCoverage() {
        ByteArrayBuilder bab = new ByteArrayBuilder(1)
            .append(SP)
            .append(CRLF_BYTES)
            .append((String)null)
            .append((ByteArrayBuilder)null)
            .append(new ByteArrayBuilder())
            .append("foo")
            .append((CharBuffer)null)
            .append(CharBuffer.wrap("bar"))
            .append(4273)
            .append(new byte[0])
            .append(new byte[0], 0)
            .append((byte)122)
            .append("baz".getBytes())
            .append(new byte[0], 0)
            .append("baz".getBytes(), 3)
            .append("baz".getBytes(), 2, 1)
            .append("baz".getBytes(), 0, 0)
            ;
        assertEquals(" \r\nnullfoonullbar4273zbazbazz", bab.toString());
        assertEquals(29, bab.length());
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.capacity());
        bab.append(bab);
        assertEquals(" \r\nnullfoonullbar4273zbazbazz \r\nnullfoonullbar4273zbazbazz", bab.toString());
        assertEquals(58, bab.length());
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.capacity());

        bab.setAllocationSize(100);
        bab.clear();
        assertEquals(0, bab.length());
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.capacity());

        bab.appendUnchecked((byte)'0');
        bab.appendUnchecked("123456789".getBytes());
        assertEquals(10, bab.length());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes(), 0, 10);
        assertEquals(60, bab.length());
        assertThrows(BufferOverflowException.class, () -> bab.appendUnchecked("12345".getBytes()));
    }

    @Test
    public void miscCoverageByteArrayPrimitiveBuilder() {
        ByteArrayPrimitiveBuilder bab = new ByteArrayPrimitiveBuilder(1)
            .append(SP)
            .append(CRLF_BYTES)
            .append((String)null)
            .append((ByteArrayPrimitiveBuilder)null)
            .append(new ByteArrayPrimitiveBuilder())
            .append("foo")
            .append((CharBuffer)null)
            .append(CharBuffer.wrap("bar"))
            .append(4273)
            .append(new byte[0])
            .append(new byte[0], 0)
            .append((byte)122)
            .append("baz".getBytes())
            .append(new byte[0], 0)
            .append("baz".getBytes(), 3)
            .append("baz".getBytes(), 2, 1)
            .append("baz".getBytes(), 0, 0)
            ;
        assertEquals(" \r\nnullfoonullbar4273zbazbazz", bab.toString());
        assertEquals(29, bab.length());
        assertEquals(DEFAULT_ASCII_ALLOCATION, bab.capacity());

        bab.append(bab);
        assertEquals(" \r\nnullfoonullbar4273zbazbazz \r\nnullfoonullbar4273zbazbazz", bab.toString());
        assertEquals(58, bab.length());
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.capacity());

        bab.setAllocationSize(DEFAULT_ASCII_ALLOCATION + 1);
        bab.clear();
        assertEquals(0, bab.length());
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.getAllocationSize());
        assertEquals(DEFAULT_ASCII_ALLOCATION * 2, bab.capacity());

        bab.appendUnchecked((byte)'0');
        bab.appendUnchecked("123456789".getBytes());
        assertEquals(10, bab.length());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes());
        bab.appendUnchecked("1234567890".getBytes());
        assertEquals(60, bab.length());
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> bab.appendUnchecked("12345".getBytes()));
    }
}
