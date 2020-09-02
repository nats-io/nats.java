package io.nats.client.impl;

import io.nats.client.Options;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.*;

public class NatsConnectionReaderTest {

    NatsConnectionReader reader;
    ProtocolHandlerMock protocolHandler;
    NatsStatisticsMock natsStatistics;

    DataPortMock dataPort;


    class ProtocolHandlerMock implements ProtocolHandler {
        Exception lastException;
        NatsMessage lastMessage;
        String lastError;
        int okCount;
        int pongCount;
        int pingCount;
        String infoJSON;

        @Override
        public void handleCommunicationIssue(Exception io) {
            lastException = io;
        }

        @Override
        public void deliverMessage(NatsMessage msg) {

            System.out.println("################### MESSAGE " + msg);

            lastMessage = msg;
        }

        @Override
        public void processOK() {
            okCount++;
        }

        @Override
        public void processError(String errorText) {
            lastError = errorText;
        }

        @Override
        public void sendPong() {
            pingCount++;
        }

        @Override
        public void handlePong() {
            pongCount++;
        }

        @Override
        public void handleInfo(String infoJson) {
            infoJSON = infoJson;
        }
    }


    class NatsStatisticsMock extends NatsStatistics {

        public NatsStatisticsMock(boolean trackAdvanced) {
            super(trackAdvanced);
        }
    }

    class DataPortMock implements DataPort {

        byte[] bytes = null;

        @Override
        public void connect(String serverURI, NatsConnection conn, long timeoutNanos) throws IOException {
        }

        @Override
        public void upgradeToSecure() throws IOException {

        }

        @Override
        public int read(byte[] dst, int off, int len) throws IOException {

            System.out.println("READ CALLED");
            if (bytes != null) {
                if (len < bytes.length) {
                    System.arraycopy(bytes, 0, dst, off, len);
                    return len;
                } else {
                    System.arraycopy(bytes, 0, dst, off, bytes.length);
                    return bytes.length;
                }
            } else {
                return 0;
            }
        }

        @Override
        public void write(byte[] src, int toWrite) throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }

    @Before
    public void setUp() throws Exception {
        protocolHandler = new ProtocolHandlerMock();
        natsStatistics = new NatsStatisticsMock(false);
        dataPort = new DataPortMock();
        reader = new NatsConnectionReader(protocolHandler, new Options.Builder().build(), natsStatistics, null);
        reader.init();
    }

    @Test
    public void runOnce() throws IOException {
        reader.runOnce(dataPort);
    }

    @Test
    public void connect() throws IOException {
        dataPort.bytes = "INFO {[\"foo\":bar]}\r\n".getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < 10; i++) {
            System.out.println("" + i + " BEFORE OP " + reader.currentOp());
            System.out.println("" + i + " BEFORE  MODE " + reader.getMode());

            reader.runOnce(dataPort);
            dataPort.bytes = null;
            System.out.println("" + i + " AFTER OP " + reader.currentOp());
            System.out.println("" + i + " AFTER MODE " + reader.getMode());
            System.out.println("" + i + " AFTER INFO " + protocolHandler.infoJSON);
            System.out.println("" + i + " AFTER MSG " + protocolHandler.lastMessage);

            if (protocolHandler.infoJSON != null) break;
        }

        assertEquals("{[\"foo\":bar]}", protocolHandler.infoJSON);
        assertNull(protocolHandler.lastError);
        assertNull(protocolHandler.lastException);

    }

    @Test
    public void message() throws IOException {
        dataPort.bytes = "MSG subj sid reply-to 1\r\nA\r\n".getBytes(StandardCharsets.UTF_8);

        for (int i = 0; i < 10; i++) {
            System.out.println("" + i + " BEFORE OP " + reader.currentOp());
            System.out.println("" + i + " BEFORE  MODE " + reader.getMode());

            reader.runOnce(dataPort);
            dataPort.bytes = null;
            System.out.println("" + i + " AFTER OP " + reader.currentOp());
            System.out.println("" + i + " AFTER MODE " + reader.getMode());
            System.out.println("" + i + " AFTER INFO " + protocolHandler.infoJSON);
            System.out.println("" + i + " AFTER MSG " + protocolHandler.lastMessage);

            if (protocolHandler.lastMessage != null) break;
        }

        assertNotNull(protocolHandler.lastMessage);
        assertNull(protocolHandler.lastError);
        assertNull(protocolHandler.lastException);

        assertEquals("subj", protocolHandler.lastMessage.getSubject());
        assertEquals("reply-to", protocolHandler.lastMessage.getReplyTo());
        assertEquals("sid", protocolHandler.lastMessage.getSID());
        assertEquals(1, protocolHandler.lastMessage.getData().length);
        assertEquals('A', protocolHandler.lastMessage.getData()[0]);

    }


    @Test
    public void hMessage() throws IOException {

        final String headers = "HEADER1: VALUE1\r\n" +
                "HEADER2: VALUE2\r\n";
        final int headerLength = headers.length();
        final String body = "A";
        final int bodyLength = body.length();
        final String subject = "subj";
        final String replyTo = "reply-to";
        final String sid = "sid";
        ////////////////////////////////////////////sb si rt hl tl    hd    payload
        final String protocol = String.format("HMSG %s %s %s %s %s\r\n%s\r\n%s\r\n",
                subject, sid, replyTo, headerLength, headerLength + bodyLength, headers, body);
        dataPort.bytes = protocol.getBytes(StandardCharsets.UTF_8);


        reader.runOnce(dataPort);

        assertNull(protocolHandler.lastError);
        assertNull(protocolHandler.lastException);

        assertEquals("subj", protocolHandler.lastMessage.getSubject());
        assertEquals("reply-to", protocolHandler.lastMessage.getReplyTo());
        assertEquals("sid", protocolHandler.lastMessage.getSID());
        assertEquals(1, protocolHandler.lastMessage.getData().length);
        assertEquals('A', protocolHandler.lastMessage.getData()[0]);

        assertEquals(2, protocolHandler.lastMessage.getHeaders().size());

        assertEquals(List.of("VALUE1"), protocolHandler.lastMessage.getHeaders().get("HEADER1"));
        assertEquals(List.of("VALUE2"), protocolHandler.lastMessage.getHeaders().get("HEADER2"));


    }


}