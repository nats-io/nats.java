package io.nats.client.support;

public class JsonUtilsLite {

    public static String jsonDecode(String s) {
        int len = s.length();
        StringBuilder sb = new StringBuilder(len);
        for (int x = 0; x < len; x++) {
            char ch = s.charAt(x);
            if (ch == '\\') {
                char nextChar = (x == len - 1) ? '\\' : s.charAt(x + 1);
                switch (nextChar) {
                    case '\\':
                        break;
                    case 'b':
                        ch = '\b';
                        break;
                    case 'f':
                        ch = '\f';
                        break;
                    case 'n':
                        ch = '\n';
                        break;
                    case 'r':
                        ch = '\r';
                        break;
                    case 't':
                        ch = '\t';
                        break;
                    // Hex Unicode: u????
                    case 'u':
                        if (x >= len - 5) {
                            ch = 'u';
                            break;
                        }
                        int code = Integer.parseInt(
                                "" + s.charAt(x + 2) + s.charAt(x + 3) + s.charAt(x + 4) + s.charAt(x + 5), 16);
                        sb.append(Character.toChars(code));
                        x += 5;
                        continue;
                    default:
                        ch = nextChar;
                        break;
                }
                x++;
            }
            sb.append(ch);
        }
        return sb.toString();
    }

    public static String jsonEncode(String s) {
        return jsonEncode(new StringBuilder(), s).toString();
    }

    public static StringBuilder jsonEncode(StringBuilder sb, String s) {
        int len = s.length();
        for (int x = 0; x < len; x++) {
            appendChar(sb, s.charAt(x));
        }
        return sb;
    }

    public static String jsonEncode(char[] chars) {
        return jsonEncode(new StringBuilder(), chars).toString();
    }

    public static StringBuilder jsonEncode(StringBuilder sb, char[] chars) {
        for (char aChar : chars) {
            appendChar(sb, aChar);
        }
        return sb;
    }

    private static void appendChar(StringBuilder sb, char ch) {
        switch (ch) {
            case '"':
                sb.append("\\\"");
                break;
            case '\\':
                sb.append("\\\\");
                break;
            case '\b':
                sb.append("\\b");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\n':
                sb.append("\\n");
                break;
            case '\r':
                sb.append("\\r");
                break;
            case '\t':
                sb.append("\\t");
                break;
            case '/':
                sb.append("\\/");
                break;
            default:
                if (ch < ' ') {
                    sb.append(String.format("\\u%04x", (int) ch));
                }
                else {
                    sb.append(ch);
                }
                break;
        }
    }
}
