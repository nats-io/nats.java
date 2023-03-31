// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.support;

public class Version implements Comparable<Version> {
    private static final String NO_EXTRA = "~";
    Integer major;
    Integer minor;
    Integer patch;
    String extra;

    public Version(String v) {
        try {
            String[] split = v.replaceAll("v", "").replaceAll("-", ".").split("\\Q.\\E");
            major = Integer.parseInt(split[0]);
            minor = Integer.parseInt(split[1]);
            patch = Integer.parseInt(split[2]);

            // tilde is the last ascii, makes 1.2.3 < 1.2.3-beta via
            // 1.2.3~ < 1.2.3-beta
            if (split.length == 3) {
                extra = NO_EXTRA;
            }
            else {
                for (int i = 3; i < split.length; i++) {
                    if (i == 3) {
                        extra = "-" + split[i];
                    }
                    else {
                        //noinspection StringConcatenationInLoop
                        extra = extra + "." + split[i];
                    }
                }
            }
        } catch (NumberFormatException nfe) {
            major = 0;
            minor = 0;
            patch = 0;
            extra = "";
        }
    }

    @Override
    public String toString() {
                                                   //noinspection StringEquality
        return major + "." + minor + "." + patch + (extra == NO_EXTRA ? "" : extra);
    }

    @Override
    public int compareTo(Version o) {
        int c = major.compareTo(o.major);
        if (c == 0) {
            c = minor.compareTo(o.minor);
            if (c == 0) {
                c = patch.compareTo(o.patch);
                if (c == 0) {
                    c = extra.compareTo(o.extra);
                }
            }
        }
        return c;
    }

    public static boolean isNewer(String v, String than) {
        return new Version(v).compareTo(new Version(than)) > 0;
    }

    public static boolean isSame(String v, String than) {
        return new Version(v).compareTo(new Version(than)) == 0;
    }

    public static boolean isOlder(String v, String than) {
        return new Version(v).compareTo(new Version(than)) < 0;
    }

    public static boolean isSameOrOlder(String v, String than) {
        return new Version(v).compareTo(new Version(than)) <= 0;
    }

    public static boolean isSameOrNewer(String v, String than) {
        return new Version(v).compareTo(new Version(than)) >= 0;
    }
}
