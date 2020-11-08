package io.nats.client.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

public abstract class ResourceUtils {

    public static List<String> getFileFromResourceAsStream(String fileName) throws IOException {
        ClassLoader classLoader = ResourceUtils.class.getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return Files.readAllLines(file.toPath());
    }
}
