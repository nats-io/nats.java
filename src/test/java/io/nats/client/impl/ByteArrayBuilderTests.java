package io.nats.client.impl;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.nats.client.utils.ResourceUtils.getFileFromResourceAsStream;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteArrayBuilderTests {

    @Test
    public void byte_array_builder_works() throws IOException {
        Random r = new Random();
        ByteArrayBuilder bbb = new ByteArrayBuilder();
        String testString = "abcdefghij";
        _test(r, bbb, Collections.singletonList(testString), StandardCharsets.US_ASCII);

        List<String> subjects = getFileFromResourceAsStream("utf8-test-strings.txt");
        bbb = new ByteArrayBuilder(StandardCharsets.UTF_8);
        _test(r, bbb, subjects, StandardCharsets.UTF_8);
    }

    private void _test(Random r, ByteArrayBuilder bbb, List<String> testStrings, Charset charset) {
        String expectedString = "";
        for (String testString : testStrings) {
            int loops = 1000 / testString.length();
            for (int x = 1; x < loops; x++) {
                String more = testString + r.nextInt(Integer.MAX_VALUE);
                bbb.append(more);
                expectedString = expectedString + more;
                byte[] bytes = bbb.toByteArray();
                assertEquals(expectedString.getBytes(charset).length, bytes.length);
                assertEquals(expectedString, new String(bytes, charset));
            }
        }
    }
}
