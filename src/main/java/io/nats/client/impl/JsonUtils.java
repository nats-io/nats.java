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
import java.util.regex.Pattern;

/**
 * Internal json parsing helpers.
 */
public final class JsonUtils {

    public enum FieldType {
        jsonBoolean,
        jsonString,
        jsonNumber,
        jsonObject, 
        jsonStringArray
    }

    private static final String grabString = "\\s*\"(.+?)\"";
    private static final String grabNumber = "\\s*(\\d+)";
    private static final String grabBoolean = "\\s*(true|false)";
    private static final String grabStringArray = "\\[(.+?)\\]";
    private static final String colon = "\"\\s*:\\s*";

    private static String getTypePattern(FieldType type) {
        switch (type) {
            case jsonBoolean :
                return grabBoolean;
            case jsonNumber :
                return grabNumber;
            case jsonString :
                return grabString;
            case jsonStringArray :
                return grabStringArray;
            default:
                return grabString;
        }
    }

    /**
     * Builds a json parsing pattern
     * @param fieldName name of the field
     * @param type type of the field.
     * @return pattern.
     */
    public static Pattern buildPattern(String fieldName, FieldType type) {
        return Pattern.compile("\""+ fieldName + colon + getTypePattern(type), Pattern.CASE_INSENSITIVE);
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
}
