package io.nats.compatibility;

public enum Kind {
    COMMAND("command"),
    RESULT("result");

    public final String name;

    Kind(String name) {
        this.name = name;
    }

    public static Kind instance(String text) {
        for (Kind os : Kind.values()) {
            if (os.name.equals(text)) {
                return os;
            }
        }
        System.err.println("Unknown kind: " + text);
        System.exit(-7);
        return null;
    }

    @Override
    public String toString() {
        return name;
    }
}
