package io.nats.compatibility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

@SuppressWarnings("CallToPrintStackTrace")
public class Utility {
    public static String RESOURCE_LOCATION = "src/test/resources/data";

    public static InputStream getFileAsInputStream(String filename) throws IOException {
        return Files.newInputStream(Paths.get(RESOURCE_LOCATION, filename));
    }
}
