package io.nats.compatibility;

public enum Suite {
    DONE("done"),
    OBJECT_STORE("object-store");

    public final String id;

    Suite(String id) {
        this.id = id;
    }

    public static Suite instance(String text) {
        for (Suite suite : Suite.values()) {
            if (suite.id.equals(text)) {
                return suite;
            }
        }
        System.err.println("Unknown suite: " + text);
        System.exit(-7);
        return null;
    }

    @Override
    public String toString() {
        return id;
    }
}
