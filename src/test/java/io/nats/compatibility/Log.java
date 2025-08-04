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

package io.nats.compatibility;

@SuppressWarnings("SameParameterValue")
public abstract class Log {
    private static final String INDENT = "    ";
    private static final String NEWLINE_INDENT = "\n" + INDENT;
    private static final boolean PRINT_THREAD_ID = true;

    private Log() {}  /* ensures cannot be constructed */

    public static void info(String label) {
        log(label, null, false);
    }

    public static void error(String label) {
        log(label, null, true);
    }

    public static void info(String label, Object... extras) {
        log(label, false, extras);
    }

    public static void error(String label, Object... extras) {
        log(label, true, extras);
    }

    public static void error(String label, Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getMessage());
        StackTraceElement[] extras = e.getStackTrace();
        for (StackTraceElement extra : extras) {
            sb.append("\n").append(INDENT).append(extra.toString());
        }
        log(label, sb.toString(), true);
    }

    private static void log(String label, boolean error, Object... extras) {
        if (extras.length == 1) {
            log(label, stringify(extras[0]), error);
        }
        else {
            boolean notFirst = false;
            StringBuilder sb = new StringBuilder();
            for (Object extra : extras) {
                String s = stringify(extra);
                if (s != null) {
                    if (notFirst) {
                        sb.append("\n");
                    }
                    else {
                        notFirst = true;
                    }
                    sb.append(s);
                }
            }
            log(label, stringify(sb), error);
        }
    }

    private static void log(String label, String extraStr, boolean error) {
        String start;
        if (PRINT_THREAD_ID) {
            String tn = Thread.currentThread().getName().replace("pool-", "p").replace("-thread-", "t");
            start = "[" + tn + "@" + time() + "] " + label;
        }
        else {
            start = "[" + time() + "] " + label;
        }

        if (extraStr == null || extraStr.isEmpty()) {
            System.out.println(start);
            return;
        }

        System.out.println(start + NEWLINE_INDENT + extraStr.replace("\n", NEWLINE_INDENT));
    }

    private static String time() {
        String t = "" + System.currentTimeMillis();
        return t.substring(t.length() - 9);
    }

    private static String stringify(Object o) {
        if (o == null) {
            return null;
        }
        if (o instanceof byte[]) {
            byte[] bytes = (byte[])o;
            if (bytes.length == 0) {
                return null;
            }
            return new String((byte[])o);
        }
        String s = o.toString().trim();
        return s.isEmpty() ? null : s;
    }
}
