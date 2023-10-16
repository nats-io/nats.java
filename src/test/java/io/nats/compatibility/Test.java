package io.nats.compatibility;

public enum Test {
    DEFAULT_BUCKET("default-bucket"),
    CUSTOM_BUCKET("custom-bucket"),
    GET_OBJECT("get-object"),
    PUT_OBJECT("put-object"),
    UPDATE_METADATA("update-metadata"),
    WATCH("watch"),
    WATCH_UPDATE("watch-updates"),
    GET_LINK("get-link"),
    PUT_LINK("put-link");

    public final String name;

    Test(String name) {
        this.name = name;
    }

    public static Test instance(String test) {
        for (Test os : Test.values()) {
            if (os.name.equals(test)) {
                return os;
            }
        }
        System.err.println("Unknown test: " + test);
        System.exit(-7);
        return null;
    }

    @Override
    public String toString() {
        return name;
    }
}
