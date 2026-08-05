package io.nats.client.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public abstract class ResourceUtils {
    public static List<String> dataAsLines(String fileName) {
        return resourceAsLines("data/" + fileName);
    }

    public static String dataAsString(String fileName) {
        return resourceAsString("data/" + fileName);
    }

    public static InputStream dataAsInputStream(String fileName) {
        return resourceAsInputStream("data/" + fileName);
    }

    public static List<String> resourceAsLines(String fileName) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(open(fileName), StandardCharsets.UTF_8))) {
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            return lines;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String resourceAsString(String fileName) {
        try (InputStream in = open(fileName)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
            return new String(out.toByteArray(), StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static InputStream resourceAsInputStream(String fileName) {
        try {
            return open(fileName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static InputStream open(String fileName) throws FileNotFoundException {
        InputStream in = ResourceUtils.class.getClassLoader().getResourceAsStream(fileName);
        if (in == null) {
            throw new FileNotFoundException(fileName);
        }
        return in;
    }

    public static String createTempFile(String prefix, String suffix, String[] lines) throws IOException {
        File f = File.createTempFile(prefix, suffix);
        BufferedWriter writer = new BufferedWriter(new FileWriter(f));
        for (String line : lines) {
            writer.write(line);
            writer.write(System.lineSeparator());
        }
        writer.flush();
        writer.close();
        return f.getAbsolutePath();
    }

    public static Path createTempDirectory() throws IOException {
        return Files.createTempDirectory(null).toAbsolutePath();
    }

    public static void deleteFileOrFolder(String path) {
        if (path != null) {
            deleteFileOrFolder(new File(path));
        }
    }

    public static void deleteFileOrFolder(Path path) {
        if (path != null) {
            deleteFileOrFolder(path.toFile());
        }
    }

    public static void deleteFileOrFolder(File file) {
        if (file != null) {
            try {
                if (file.isDirectory()) {
                    File[] entries = file.listFiles();
                    if (entries != null) {
                        for (File entry : entries) {
                            deleteFileOrFolder(entry);
                        }
                    }
                }
                //noinspection ResultOfMethodCallIgnored
                file.delete();
            }
            catch (Exception ignore) {}
        }
    }
}
