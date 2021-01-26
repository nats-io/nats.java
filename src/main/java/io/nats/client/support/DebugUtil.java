package io.nats.client.support;

public class DebugUtil {
    public static String printable(Object o) {
        return printable(o.toString());
    }

    public static String printable(String s) {
        String indent = "";
        boolean inPrimitiveArray = false;
        boolean inObjectArray = false;
        boolean lastEq = false;
        boolean lastComma = false;
        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < s.length(); x++) {
            char c = s.charAt(x);
            if (c == '=') {
                lastEq = true;
                sb.append(": ");
            } else {
                if (c == '{') {
                    indent += "  ";
                    sb.append(":\n").append(indent);
                } else if (c == '}') {
                    indent = indent.substring(0, indent.length() - 2);
                } else if (c == '[') {
                    if (lastEq) {
                        inPrimitiveArray = true;
                        indent += "  ";
                        sb.append("\n").append(indent).append("- ");
                    } else if (lastComma) {
                        inObjectArray = true;
                        lastComma = false;
                        indent += "  ";
                        sb.append("[\n").append(indent);
                    } else {
                        sb.append(c);
                    }
                } else if (c == ']') {
                    if (inPrimitiveArray) {
                        inPrimitiveArray = false;
                        indent = indent.substring(0, indent.length() - 2);
                    } else if (inObjectArray) {
                        inObjectArray = false;
                        indent = indent.substring(0, indent.length() - 2);
                        sb.append('\n').append(indent).append(']');
                    } else {
                        sb.append(c);
                    }
                } else if (c == ',') {
                    sb.append("\n").append(indent);
                    if (inPrimitiveArray) {
                        sb.append("- ");
                    } else {
                        lastComma = true;
                    }
                    x++;
                } else {
                    sb.append(c);
                    if (!Character.isWhitespace(c)) {
                        lastComma = false;
                    }
                }
                lastEq = false;
            }
        }
        return sb.toString().replaceAll("'null'", "null");
    }
}
