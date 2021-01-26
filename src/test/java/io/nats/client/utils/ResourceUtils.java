package io.nats.client.utils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public abstract class ResourceUtils {
    public static List<String> dataAsLines(String fileName) {
        return resourceAsLines("data/" + fileName);
    }

    public static String dataAsString(String fileName) {
        return resourceAsString("data/" + fileName);
    }

    public static List<String> resourceAsLines(String fileName) {
        try {
            ClassLoader classLoader = ResourceUtils.class.getClassLoader();
            File file = new File(classLoader.getResource(fileName).getFile());
            return Files.readAllLines(file.toPath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static String resourceAsString(String fileName) {
        try {
            ClassLoader classLoader = ResourceUtils.class.getClassLoader();
            File file = new File(classLoader.getResource(fileName).getFile());
            return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
