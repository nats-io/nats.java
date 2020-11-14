// Copyright 2015-2018 The NATS Authors
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

import java.util.*;

import static java.nio.charset.StandardCharsets.US_ASCII;


public class Headers {
	private static String COLON = ":";
	private static String COLON_SPACE = ": ";
	private static String CRLF = "\r\n";
	private static String HEADER_PREFIX = "NATS/1.0\r\n";

	private static final String KEY_CANNOT_BE_EMPTY_OR_NULL = "Header key cannot be null.";
	private static final String KEY_INVALID_CHARACTER = "Header key has invalid character: ";
	private static final String VALUE_INVALID_CHARACTERS = "Header value has invalid character: ";

	private static final boolean normalizeKey = false;

	private Map<String, List<String>> headerMap = new HashMap<>();
	private byte[] serialized;

	public int serializedLength() {
		return getSerialized().length;
	}

	public byte[] getSerialized() {
		if (serialized == null) {
			StringBuilder sb = new StringBuilder(HEADER_PREFIX);
			for (String key : headerMap.keySet()) {
				for (String value : values(key)) {
					sb.append(key);
					sb.append(COLON);
					sb.append(value);
					sb.append(CRLF);
				}
			}
			sb.append(CRLF);
			serialized = sb.toString().getBytes(US_ASCII);
		}
		return serialized;
	}

	public String getValueCsv(String key) {
		return String.join(",", values(key));
	}

	/**
	 * If the key is present add the values to the list of values for the key.
	 * If the key is not present, sets the specified values for the key.
	 * null values are ignored. If all values are null, the key is not added or updated.
	 *
	 * @param key the key
	 * @param values the values
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers add(String key, String... values) {
		if (values != null) {
			_add(key, Arrays.asList(values));
		}
		return this;
	}

	/**
	 * If the key is present add the values to the list of values for the key.
	 * If the key is not present, sets the specified values for the key.
	 * null values are ignored. If all values are null, the key is not added or updated.
	 *
	 * @param key the key
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers add(String key, Collection<String> values) {
		_add(key, values);
		return this;
	}

	// the add delegate
	private void _add(String key, Collection<String> values) {
		if (values != null) {
			String normalizedKey = checkAndNormalizeKey(key);
			List<String> checked = checkValues(values);
			if (!checked.isEmpty()) {
				List<String> currentSet = headerMap.get(normalizedKey);
				if (currentSet == null) {
					headerMap.put(normalizedKey, checked);
				} else {
					currentSet.addAll(checked);
				}
				serialized = null; // since the data changed, clear this so it's rebuilt
			}
		}
	}

	/**
	 * Associates the specified values with the key. If the key was already present
	 * any existing values are removed and replaced with the new list.
	 * null values are ignored. If all values are null, the put is ignored
	 *
	 * @param key the key
	 * @param values the values
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers put(String key, String... values) {
		if (values != null) {
			_put(key, Arrays.asList(values));
		}
		return this;
	}

	/**
	 * Associates the specified values with the key. If the key was already present
	 * any existing values are removed and replaced with the new list.
	 * null values are ignored. If all values are null, the put is ignored
	 *
	 * @param key the key
	 * @param values the values
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers put(String key, Collection<String> values) {
		_put(key, values);
		return this;
	}

	// the put delegate
	private void _put(String key, Collection<String> values) {
		if (values != null) {
			String normalizedKey = checkAndNormalizeKey(key);
			List<String> checked = checkValues(values);
			if (!checked.isEmpty()) {
				headerMap.put(normalizedKey, checked);
				serialized = null; // since the data changed, clear this so it's rebuilt
			}
		}
	}

	/**
	 * Removes each key and its values if the key was present
	 *
	 * @param keys the key or keys to remove
	 */
	public void remove(String... keys) {
		for (String key : keys) {
			headerMap.remove(nomalizeKey(key));
		}
		serialized = null; // since the data changed, clear this so it's rebuilt
	}

	/**
	 * Removes each key and its values if the key was present
	 *
	 * @param keys the key or keys to remove
	 */
	public void remove(Collection<String> keys) {
		for (String key : keys) {
			headerMap.remove(nomalizeKey(key));
		}
		serialized = null; // since the data changed, clear this so it's rebuilt
	}

	public int size() {
		return headerMap.size();
	}

	public boolean isEmpty() {
		return headerMap.isEmpty();
	}

	public void clear() {
		headerMap.clear();
	}

	public boolean containsKey(String key) {
		return headerMap.containsKey(nomalizeKey(key));
	}

	public List<String> values(String key) {
		List<String> list = headerMap.get(nomalizeKey(key));
		return list == null ? null : Collections.unmodifiableList(list);
	}

	private String checkAndNormalizeKey(String key) {
		// key cannot be null or empty and contain only printable characters except colon
		if (key == null || key.length() == 0) {
			throw new IllegalArgumentException(KEY_CANNOT_BE_EMPTY_OR_NULL);
		}

		key.chars().forEach(c -> {
			if (c < 33 || c > 126 || c == ':') {
				throw new IllegalArgumentException(KEY_INVALID_CHARACTER + "'" + c + "'");
			}
		});

		return nomalizeKey(key);
	}

	/*
		Header field names are case-insensitive, become a requirement in NATS header fields:
	    First character in a header field is capitalized if in the range of [a-z]
    	First characters following a dash (-) are capitalized if in the range of [a-z]
	 */
	public String nomalizeKey(String key) {
		return normalizeKey ? normalizeSpec(key) : key;
	}

	public static String normalizeSpec(String key) {
		if (key.toLowerCase().equals("-aa")) {
			int z = 0;
		}
		int len = key.length();
		char c = key.charAt(0);
		if (len == 1) {
			// a - z become A - Z, everything else no change
			if (c > 96 && c < 123) { // if lowercase,upper it
				return new String(new char[] {(char)(c - 32)}, 0, len); // saves one stack push by providing offset and count
			}
			return key;
		}

		boolean upNext = true;
		char[] normalized = new char[len];
		for (int idx = 0; idx < len; idx++) {
			c = key.charAt(idx);
			if (c == '-') {
				upNext = true;
				normalized[idx] = c;
			}
			else if (upNext) {
				upNext = false;
				if (c > 96 && c < 123) { // if lowercase,upper it
					normalized[idx] = (char)(c - 32);
				}
				else {
					normalized[idx] = c;
				}
			}
			else {
				normalized[idx] = c;
			}
		}
		return new String(normalized, 0, len); // saves one stack push by providing offset and count
	}

	public String nomalizeSpecx(String key) {
		// key is assumed validated to not be null or empty and printable
		// A=65 Z=90 a=97=A+32 z=122=Z+32
		int len = key.length();
		char c = key.charAt(0);
		if (len == 1) {
			// a - z become A - Z, everything else no change
			if (c > 96 && c < 123) { // if lowercase,upper it
				return new String(new char[] {(char)(c - 32)}, 0, len); // saves one stack push by providing offset and count
			}
			return key;
		}

		// if the first char is dash, leave it bu check the second character
		char[] normalized = new char[len];
		int idx = 0;
		if (c == '-') {
			normalized[0] = c;
			idx = 1;
		}

		// first [or second if first was dash] character must be upper if lower
		c = key.charAt(idx);
		if (c > 96 && c < 123) { // if lowercase,upper it
			normalized[idx] = (char)(c - 32);
		}
		else {
			normalized[idx] = c;
		}
		idx++;

		for (; idx < len; idx++) {
			c = key.charAt(idx);
			if (c > 64 && c < 91) { // if uppercase, lower it
				normalized[idx] = (char)(c + 32);
			}
			else {
				normalized[idx] = c;
			}
		}
		return new String(normalized, 0, len); // saves one stack push by providing offset and count
	}

	private List<String> checkValues(Collection<String> values) {

		List<String> checked = new ArrayList<>();
		if (values != null && !values.isEmpty()) {
			for (String v : values) {
				if ( checkValue(v) ) {
					checked.add(v);
				}
			}
		}
		return checked;
	}

	private boolean checkValue(String val) {
		// Generally more permissive than HTTP.  Allow only printable
		// characters and include tab (0x9) to cover what's allowed
		// in quoted strings and comments.
		// null is just ignored
		if (val == null) {
			return false;
		}
		if (!val.isEmpty()) {
			val.chars().forEach(c -> {
				if ((c < 32 && c != 9) || c > 126) {
					throw new IllegalArgumentException(VALUE_INVALID_CHARACTERS + c);
				}
			});
		}
		return true;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Headers headers = (Headers) o;
		return Objects.equals(headerMap, headers.headerMap);
	}

	@Override
	public int hashCode() {
		return Objects.hash(headerMap);
	}
}
