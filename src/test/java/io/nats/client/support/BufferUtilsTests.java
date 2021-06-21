package io.nats.client.support;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

import io.nats.client.channels.ByteBufferChannel;

import static io.nats.client.support.BufferUtils.hexdump;
import static io.nats.client.support.BufferUtils.readLine;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BufferUtilsTests {
    @Test
    public void testHexdump() {
        byte[] allBytes = new byte[257];
        for (int i=0; i < 256; i++) {
            allBytes[i] = (byte)i;
        }
        assertEquals(
            "0000  00 01 02 03 04 05 06 07  08 09 0A 0B 0C 0D 0E 0F  ␣␣␣␣␣␣␣␣␣␉␤␣␣␍␣␣\n" +
            "0010  10 11 12 13 14 15 16 17  18 19 1A 1B 1C 1D 1E 1F  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "0020  20 21 22 23 24 25 26 27  28 29 2A 2B 2C 2D 2E 2F  ␠!\"#$%&'()*+,-./\n" +
            "0030  30 31 32 33 34 35 36 37  38 39 3A 3B 3C 3D 3E 3F  0123456789:;<=>?\n" +
            "0040  40 41 42 43 44 45 46 47  48 49 4A 4B 4C 4D 4E 4F  @ABCDEFGHIJKLMNO\n" +
            "0050  50 51 52 53 54 55 56 57  58 59 5A 5B 5C 5D 5E 5F  PQRSTUVWXYZ[\\]^_\n" +
            "0060  60 61 62 63 64 65 66 67  68 69 6A 6B 6C 6D 6E 6F  `abcdefghijklmno\n" +
            "0070  70 71 72 73 74 75 76 77  78 79 7A 7B 7C 7D 7E 7F  pqrstuvwxyz{|}~␣\n" +
            "0080  80 81 82 83 84 85 86 87  88 89 8A 8B 8C 8D 8E 8F  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "0090  90 91 92 93 94 95 96 97  98 99 9A 9B 9C 9D 9E 9F  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "00a0  A0 A1 A2 A3 A4 A5 A6 A7  A8 A9 AA AB AC AD AE AF  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "00b0  B0 B1 B2 B3 B4 B5 B6 B7  B8 B9 BA BB BC BD BE BF  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "00c0  C0 C1 C2 C3 C4 C5 C6 C7  C8 C9 CA CB CC CD CE CF  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "00d0  D0 D1 D2 D3 D4 D5 D6 D7  D8 D9 DA DB DC DD DE DF  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "00e0  E0 E1 E2 E3 E4 E5 E6 E7  E8 E9 EA EB EC ED EE EF  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "00f0  F0 F1 F2 F3 F4 F5 F6 F7  F8 F9 FA FB FC FD FE FF  ␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣␣\n" +
            "0100  00                                                ␣\n",
            hexdump(ByteBuffer.wrap(allBytes), 0, allBytes.length));
    }

    @Test
    public void testNullReadLine() throws IOException {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(1024));
        channel.write(ByteBuffer.wrap("Hello\nWorld".getBytes(UTF_8)));
        assertThrows(NullPointerException.class, () -> readLine(null, channel));
    }

    @Test
    public void testReadLineSmallBuffer() throws IOException {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(1024));
        channel.write(ByteBuffer.wrap("Hello\n\nWorld".getBytes(UTF_8)));
        ByteBuffer tmp = ByteBuffer.allocate(8);
        assertEquals("Hello", readLine(tmp, channel));
        assertEquals("", readLine(tmp, channel));
        assertEquals("World", readLine(tmp, channel));
        assertNull(readLine(tmp, channel));
    }

    @Test
    public void testReadLineCRLF() throws IOException {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(1024));
        channel.write(ByteBuffer.wrap("Hello\r\n\r\nWorld\r\n".getBytes(UTF_8)));
        ByteBuffer tmp = ByteBuffer.allocate(8);
        assertEquals("Hello", readLine(tmp, channel));
        assertEquals("", readLine(tmp, channel));
        assertEquals("World", readLine(tmp, channel));
        assertNull(readLine(tmp, channel));
    }

    @Test
    public void testReadLineCR() throws IOException {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(1024));
        channel.write(ByteBuffer.wrap("Hello\r\rWorld\r".getBytes(UTF_8)));
        ByteBuffer tmp = ByteBuffer.allocate(8);
        assertEquals("Hello", readLine(tmp, channel));
        assertEquals("", readLine(tmp, channel));
        assertEquals("World", readLine(tmp, channel));
        assertNull(readLine(tmp, channel));
    }

    @Test
    public void testFinalNewline() throws IOException {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(1024));
        channel.write(ByteBuffer.wrap("Hello\nWorld\n".getBytes(UTF_8)));
        ByteBuffer tmp = ByteBuffer.allocate(8);
        assertEquals("Hello", readLine(tmp, channel));
        assertEquals("World", readLine(tmp, channel));
        assertNull(readLine(tmp, channel));
    }

    @Test
    public void testBufferUnderflow() throws IOException {
        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(1024));
        channel.write(ByteBuffer.wrap("Hello\nWorld".getBytes(UTF_8)));
        ByteBuffer tmp = ByteBuffer.allocate(3);
        assertThrows(BufferUnderflowException.class, () -> {
            String result = readLine(tmp, channel);
            throw new RuntimeException("unexpected result=" + result);
        });
    }

    @Test
    public void testReadLineNullChannel() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put(ByteBuffer.wrap("Hello\nWorld".getBytes(UTF_8)));
        assertEquals("Hello", readLine(buffer, null));
        assertEquals("World", readLine(buffer, null));
        assertNull(readLine(buffer, null));
    }

    @Test
    public void testReadLineBufferAndChannel() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        buffer.put(ByteBuffer.wrap("Hell".getBytes(UTF_8)));

        ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(1024));
        channel.write(ByteBuffer.wrap("o\nWorld".getBytes(UTF_8)));
        assertEquals("Hello", readLine(buffer, channel));
        assertEquals("World", readLine(buffer, channel));
        assertNull(readLine(buffer, channel));
    }
}
