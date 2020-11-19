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

import java.util.*;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * An object that represents a map of keys to a list of values. It does not accept
 * null or invalid keys. It ignores null values, accepts empty string as a value
 * and rejects invalid values.
 *
 * THIS CLASS IS NOT THREAD SAFE
 */
public class Headers {
	private static final String VERSION = "NATS/1.0";
	private static final String EMPTY = "";
	private static final String CRLF = "\r\n";
	private static final byte[] VERSION_BYTES = VERSION.getBytes(US_ASCII);
	private static final byte[] VERSION_BYTES_PLUS_CRLF = (VERSION + "\r\n").getBytes(US_ASCII);
	private static final byte[] COLON_BYTES = ":".getBytes(US_ASCII);
	private static final byte[] CRLF_BYTES = CRLF.getBytes(US_ASCII);
	private static final byte SP = ' ';
	private static final byte CR = '\r';
	private static final byte LF = '\n';
	private static final int VERSION_BYTES_LEN = VERSION_BYTES.length;
	private static final int VERSION_BYTES_PLUS_CRLF_LEN = VERSION_BYTES_PLUS_CRLF.length;
	private static final int COLON_BYTES_LEN = COLON_BYTES.length;
	private static final int CRLF_BYTES_LEN = CRLF_BYTES.length;

	private static final String KEY_CANNOT_BE_EMPTY_OR_NULL = "Header key cannot be null.";
	private static final String KEY_INVALID_CHARACTER = "Header key has invalid character: ";
	private static final String VALUE_INVALID_CHARACTERS = "Header value has invalid character: ";
	private static final String INVALID_HEADER_VERSION = "Invalid header version";
	private static final String INVALID_HEADER_COMPOSITION = "Invalid header composition";
	private static final String SERIALIZED_HEADER_CANNOT_BE_NULL_OR_EMPTY = "Serialized header cannot be null or empty.";

	private final Map<String, List<String>> headerMap;
	private byte[] serialized;

	public Headers() {
		headerMap = new HashMap<>();
	}

	public Headers(byte[] serialized) {
		// basic validation first to help fail fast
		if (serialized == null || serialized.length == 0) {
			throw new IllegalArgumentException(SERIALIZED_HEADER_CANNOT_BE_NULL_OR_EMPTY);
		}

		for (int x = 0; x < VERSION_BYTES_LEN; x++) {
			if (serialized[x] != VERSION_BYTES[x]) {
				throw new IllegalArgumentException(INVALID_HEADER_VERSION);
			}
		}

		int len = serialized.length;
		if (serialized[len - 2] != CR ||
				serialized[len - 1] != LF) {
			throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
		}

		if (serialized[VERSION_BYTES_LEN] == SP) {
			// INLINE STATUS
			String s = new String(serialized, VERSION_BYTES_LEN + 1, len - VERSION_BYTES_LEN - 1, US_ASCII).trim();
			if (s.length() == 0) {
				throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
			}
			String key;
			String value;
			int at = s.indexOf(SP);
			if (at == -1) {
				key = s;
				value = EMPTY;
			}
			else {
				key = s.substring(0, at);
				value = s.substring(at + 1).trim();
			}
			headerMap = new HashMap<>();
			add(key, value);
		}
		else {
			// REGULAR HEADER
			// Version ends in crlf
			if (serialized[VERSION_BYTES_LEN] != CR || serialized[VERSION_BYTES_LEN + 1] != LF) {
				throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
			}

			// ends in \r\n\r\n, I check here b/c the string split function doesn't give empty strings
			if (serialized[len - 4] != CR ||
					serialized[len - 3] != LF) {
				throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
			}

			headerMap = new HashMap<>();
			String[] split = new String(serialized, VERSION_BYTES_LEN + 2, len - VERSION_BYTES_LEN - 2, US_ASCII).split("\\Q\r\n\\E");
			if (split.length == 0) {
				throw new IllegalArgumentException(INVALID_HEADER_COMPOSITION);
			}
			for (int x = 0; x < split.length; x++) {
				String[] pair = split[x].split(":");
				if (pair.length == 1) {
					add(pair[0], EMPTY);
				}
				else {
					add(pair[0], pair[1].trim().split(","));
				}
			}
		}
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
			checkKey(key);
			List<String> checked = checkValues(values);
			if (!checked.isEmpty()) {
				List<String> currentSet = headerMap.get(key);
				if (currentSet == null) {
					headerMap.put(key, checked);
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

	// the put delegate that all puts call
	private void _put(String key, Collection<String> values) {
		if (values != null) {
			checkKey(key);
			List<String> checked = checkValues(values);
			if (!checked.isEmpty()) {
				headerMap.put(key, checked);
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
			headerMap.remove(key);
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
			headerMap.remove(key);
		}
		serialized = null; // since the data changed, clear this so it's rebuilt
	}

	/**
	 * Returns the number of keys in the header.
	 *
	 * @return the number of keys
	 */
	public int size() {
		return headerMap.size();
	}

	/**
	 * Returns <tt>true</tt> if this map contains no keys.
	 *
	 * @return <tt>true</tt> if this map contains no keyss
	 */
	public boolean isEmpty() {
		return headerMap.isEmpty();
	}

	/**
	 * Removes all of the keys The object map will be empty after this call returns.
	 */
	public void clear() {
		headerMap.clear();
	}

	/**
	 * Returns <tt>true</tt> if key is present (has values)
	 *
	 * @param key key whose presence is to be tested
	 * @return <tt>true</tt> if the key is present (has values)
	 */
	public boolean containsKey(String key) {
		return headerMap.containsKey(key);
	}

	/**
	 * Returns a {@link Set} view of the keys contained in the object.
	 *
	 * @return a read-only set the keys contained in this map
	 */
	public Set<String> keySet() {
		return Collections.unmodifiableSet(headerMap.keySet());
	}

	/**
	 * Returns a {@link List} view of the values for the specific key.
	 * Will be {@code null} if the key is not found.
	 *
	 * @return a read-only list of the values for the specific keys.
	 */
	public List<String> values(String key) {
		List<String> list = headerMap.get(key);
		return list == null ? null : Collections.unmodifiableList(list);
	}

	/**
	 * Returns the number of bytes that will be in the serialized version.
	 *
	 * @return the number of bytes
	 */
	public int serializedLength() {
		return getSerialized().length;
	}

	public byte[] getSerialized() {
		if (serialized == null) {
			ByteArrayBuilder bab = new ByteArrayBuilder()
					.append(VERSION_BYTES_PLUS_CRLF, VERSION_BYTES_PLUS_CRLF_LEN);
			for (String key : headerMap.keySet()) {
				for (String value : values(key)) {
					bab.append(key);
					bab.append(COLON_BYTES, COLON_BYTES_LEN);
					bab.append(value);
					bab.append(CRLF_BYTES, CRLF_BYTES_LEN);
				}
			}
			bab.append(CRLF_BYTES, CRLF_BYTES_LEN);
			serialized = bab.toByteArray();
		}
		return serialized;
	}

	/**
	 * Check the key to ensure it matches the specification for keys.
	 *
	 * @throws IllegalArgumentException if the key is null, empty or contains
	 *         an invalid character
	 */
	private void checkKey(String key) {
		// key cannot be null or empty and contain only printable characters except colon
		if (key == null || key.length() == 0) {
			throw new IllegalArgumentException(KEY_CANNOT_BE_EMPTY_OR_NULL);
		}

		int len = key.length();
		for (int idx = 0; idx < len; idx++) {
			char c = key.charAt(idx);
			if (c < 33 || c > 126 || c == ':') {
				throw new IllegalArgumentException(KEY_INVALID_CHARACTER + "'" + c + "'");
			}
		}
	}

	/**
	 * Check values to ensure they match the specification for values. Returns a list of
	 * valid values ignoring values that are null or empty strings. This may make the
	 * return list empty. The method throws an exception if any non null value is invalid.
	 * Empty string is a valid value.
	 *
	 * @return the list of valid values
	 * @throws IllegalArgumentException if the value contains an invalid character
	 */
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

	/**
	 * Check a value to ensure that it matches the specification for values.
	 *
	 * @return false if the value is null, true if it is valid
	 * @throws IllegalArgumentException if the value contains an invalid character
	 */
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
