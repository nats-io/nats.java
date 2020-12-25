// Copyright 2020 The NATS Authors
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

package io.nats.client.impl;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Internal json parsing helpers.
 */
public final class JsonUtils {

    private static final String OBJECT_RE = "\\{(.+?)\\}";
    private static final String STRING_RE  = "\\s*\"(.+?)\"";
    private static final String BOOLEAN_RE =  "\\s*(true|false)";
    private static final String NUMBER_RE =  "\\s*(\\d+)";
    private static final String STRING_ARRAY_RE_JsonUtils =      "\\[\\s*(.+?)\\s*\\]";
    private static final String STRING_ARRAY_RE_NatsServerInfo = "\\s*\\[(\".+?\")\\]";
    private static final String STRING_ARRAY_RE =                "\\s*\\[\\s*(\".+?\")\\s*\\]";
    private static final String BEFORE_FIELD = "\"";
    private static final String AFTER_FIELD = "\"\\s*:\\s*";

    public enum FieldType {
        jsonObject(OBJECT_RE),
        jsonString(STRING_RE),
        jsonBoolean(BOOLEAN_RE),
        jsonNumber(NUMBER_RE),
        jsonStringArray(STRING_ARRAY_RE);

        final String re;
        FieldType(String re) {
            this.re = re;
        }
    }

    private static final DateTimeFormatter rfc3339Formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.nnnnnnnnn");

    public static Pattern buildCustomPattern(String re) {
        return Pattern.compile(re, Pattern.CASE_INSENSITIVE);
    }

    public static Pattern buildObjectPattern() {
        return Pattern.compile(OBJECT_RE, Pattern.CASE_INSENSITIVE);
    }

    public static Pattern buildStringPattern(String field) {
        return buildPattern(field, STRING_RE);
    }

    public static Pattern buildNumberPattern(String field) {
        return buildPattern(field, NUMBER_RE);
    }

    public static Pattern buildBooleanPattern(String field) {
        return buildPattern(field, BOOLEAN_RE);
    }

    public static Pattern buildStringArrayPattern(String field) {
        return buildPattern(field, STRING_ARRAY_RE);
    }

    /**
     * Builds a json parsing pattern
     * @param fieldName name of the field
     * @param type type of the field.
     * @return pattern.
     */
    public static Pattern buildPattern(String fieldName, FieldType type) {
        return buildPattern(fieldName, type.re);
    }

    public static Pattern buildPattern(String fieldName, String typeRE) {
        return Pattern.compile(BEFORE_FIELD + fieldName + AFTER_FIELD + typeRE, Pattern.CASE_INSENSITIVE);
    }

    /**
     * Internal method to get a JSON object.
     * @param objectName - object name
     * @param json - json
     * @return string with object json
     */
    public static String getJSONObject(String objectName, String json) {

        int objStart = json.indexOf(objectName);
        if (objStart < 0) {
            return null;
        }
    
        int bracketStart = json.indexOf("{", objStart);
        int bracketEnd = json.indexOf("}", bracketStart);
    
        if (bracketStart < 0 || bracketEnd < 0) {
            return null;
        }
    
        return json.substring(bracketStart, bracketEnd+1);
    }

    /**
     * Parses JSON string array field.
     * @param fieldName - name of the field.
     * @param json - JSON that contains this struct.
     * @return a string array, empty if no values are found.
     */
    public static String[] parseStringArray(String fieldName, String json) {
        Pattern regex = JsonUtils.buildPattern(fieldName, FieldType.jsonStringArray);
        Matcher m = regex.matcher(json);
        if (!m.find()) {
            return new String[0];
        }
        String jsonArray = m.group(1);
        String[] quotedStrings = jsonArray.split("\\s*,\\s*");
        String[] rv = new String[quotedStrings.length];
        for (int i = 0; i < quotedStrings.length; i++) {
            // subjects cannot contain quotes, so just do a replace.
            rv[i] = quotedStrings[i].replace("\"", "");
        }
        return rv;     
    }    

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFld(StringBuilder sb, String fname, String value) {
        if (value != null) {
            sb.append("\"" + fname + "\" : \"" + value + "\",");
        }
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFld(StringBuilder sb, String fname, boolean value) {
        sb.append("\"" + fname + "\":" + (value ? "true" : "false") + ",");
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */    
    public static void addFld(StringBuilder sb, String fname, long value) {
        if (value >= 0) {
            sb.append("\"" + fname + "\" : " + value + ",");
        }      
    }
    
    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */
    public static void addFld(StringBuilder sb, String fname, Duration value) {
        if (value != Duration.ZERO) {
            sb.append("\"" + fname + "\" : " + value.toNanos() + ",");
        }       
    }

    /**
     * Appends a json field to a string builder.
     * @param sb string builder
     * @param fname fieldname
     * @param value field value
     */    
    public static void addFld(StringBuilder sb, String fname, String[] strArray) {
        if (strArray == null || strArray.length == 0) {
            return;
        }

        sb.append("\"" + fname  + "\":[");
        for (int i = 0; i < strArray.length; i++) {
            sb.append("\"" + strArray[i] + "\"");
            if (i < strArray.length-1) {
                sb.append(",");
            }
        }
        sb.append("],");
    }

    /**
     * Appends a date/time to a string builder as a rfc 3339 formatted field.
     * @param sb string builder
     * @param fname fieldname
     * @param time field value
     */
    public static void addFld(StringBuilder sb, String fname, ZonedDateTime time) {
        if (time == null) {
            return;
        }

        String s = rfc3339Formatter.format(time);
        sb.append("\"" + fname + "\" : \"" + s + "Z\",");
    }    

    /**
     * Parses a date time from the server.
     * @param dateTime - date time from the server.
     * @return a Zoned Date time.
     */
    public static ZonedDateTime parseDateTime(String dateTime) {
        Instant inst = Instant.parse(dateTime);
        return ZonedDateTime.ofInstant(inst, ZoneId.systemDefault());
    }
}
