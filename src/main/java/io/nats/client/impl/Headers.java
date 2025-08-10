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

import io.nats.client.support.ByteArrayBuilder;
import org.jspecify.annotations.Nullable;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static io.nats.client.support.NatsConstants.*;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * An object that represents a map of keys to a list of values. It does not accept
 * null or invalid keys. It ignores null values, accepts empty string as a value
 * and rejects invalid values.
 * !!!
 * THIS CLASS IS NOT THREAD SAFE
 */
public class Headers {

	private static final String KEY_CANNOT_BE_EMPTY_OR_NULL = "Header key cannot be null.";
	private static final String KEY_INVALID_CHARACTER = "Header key has invalid character: ";
	private static final String VALUE_INVALID_CHARACTERS = "Header value has invalid character: ";

	private final Map<String, List<String>> valuesMap;
	private final Map<String, Integer> lengthMap;
	private byte @Nullable [] serialized;
	private int dataLength;

	public Headers() {
		this(null, false, null);
	}

	public Headers(Headers headers) {
		this(headers, false, null);
	}

	public Headers(Headers headers, boolean readOnly) {
		this(headers, readOnly, null);
	}

	public Headers(@Nullable Headers headers, boolean readOnly, String @Nullable [] keysNotToCopy) {
		Map<String, List<String>> tempValuesMap = new HashMap<>();
		Map<String, Integer> tempLengthMap = new HashMap<>();
		if (headers != null) {
			tempValuesMap.putAll(headers.valuesMap);
			tempLengthMap.putAll(headers.lengthMap);
			dataLength = headers.dataLength;
			if (keysNotToCopy != null) {
				for (String key : keysNotToCopy) {
					if (key != null) {
						if (tempValuesMap.remove(key) != null) {
							dataLength -= tempLengthMap.remove(key);
						}
					}
				}
			}
		}
		if (readOnly) {
			valuesMap = Collections.unmodifiableMap(tempValuesMap);
			lengthMap = Collections.unmodifiableMap(tempLengthMap);
		}
		else {
			valuesMap = tempValuesMap;
			lengthMap = tempLengthMap;
		}
	}

	/**
	 * If the key is present add the values to the list of values for the key.
	 * If the key is not present, sets the specified values for the key.
	 * null values are ignored. If all values are null, the key is not added or updated.
	 *
	 * @param key the key
	 * @param values the values
	 * @return the Headers object
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers add(String key, String... values) {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		if (values == null || values.length == 0) {
			return this;
		}
		return _add(key, Arrays.asList(values));
	}

	/**
	 * If the key is present add the values to the list of values for the key.
	 * If the key is not present, sets the specified values for the key.
	 * null values are ignored. If all values are null, the key is not added or updated.
	 *
	 * @param key the entry key
	 * @param values a list of values to the entry
	 * @return the Header object
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers add(String key, Collection<String> values) {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		if (values == null || values.isEmpty()) {
			return this;
		}
		return _add(key, values);
	}

	// the add delegate
	private Headers _add(String key, Collection<String> values) {
		if (values != null) {
			Checker checked = new Checker(key, values);
			if (checked.hasValues()) {
				// get values by key or compute empty if absent
				// update the data length with the additional len
				// update the lengthMap for the key to the old length plus the new length
				List<String> currentSet = valuesMap.computeIfAbsent(key, k -> new ArrayList<>());
				currentSet.addAll(checked.list);
				dataLength += checked.len;
				int oldLen = lengthMap.getOrDefault(key, 0);
				lengthMap.put(key, oldLen + checked.len);
				serialized = null; // since the data changed, clear this so it's rebuilt
			}
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
	 * @return the Headers object
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers put(String key, String... values) {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		if (values == null || values.length == 0) {
			return this;
		}
		return _put(key, Arrays.asList(values));
	}

	/**
	 * Associates the specified values with the key. If the key was already present
	 * any existing values are removed and replaced with the new list.
	 * null values are ignored. If all values are null, the put is ignored
	 *
	 * @param key the key
	 * @param values the values
	 * @return the Headers object
	 * @throws IllegalArgumentException if the key is null or empty or contains invalid characters
	 *         -or- if any value contains invalid characters
	 */
	public Headers put(String key, Collection<String> values) {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		if (values == null || values.isEmpty()) {
			return this;
		}
		return _put(key, values);
	}

	/**
	 * Associates all specified values with their key. If the key was already present
	 * any existing values are removed and replaced with the new list.
	 * null values are ignored. If all values are null, the put is ignored
	 * @param map the map
	 * @return the Headers object
	 */
	public Headers put(Map<String, List<String>> map) {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		if (map == null || map.isEmpty()) {
			return this;
		}
		for (Map.Entry<String, List<String>> entry : map.entrySet()) {
			_put(entry.getKey(), entry.getValue());
		}
		return this;
	}

	// the put delegate
	private Headers _put(String key, Collection<String> values) {
		if (key == null || key.isEmpty()) {
			throw new IllegalArgumentException("Key cannot be null or empty.");
		}
		if (values != null) {
			Checker checked = new Checker(key, values);
			if (checked.hasValues()) {
				// update the data length removing the old length adding the new length
				// put for the key
				dataLength = dataLength - lengthMap.getOrDefault(key, 0) + checked.len;
				valuesMap.put(key, checked.list);
				lengthMap.put(key, checked.len);
				serialized = null; // since the data changed, clear this so it's rebuilt
			}
		}
		return this;
	}

	/**
	 * Removes each key and its values if the key was present
	 *
	 * @param keys the key or keys to remove
	 */
	public void remove(String... keys) {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		for (String key : keys) {
			_remove(key);
		}
		serialized = null; // since the data changed, clear this so it's rebuilt
	}

	/**
	 * Removes each key and its values if the key was present
	 *
	 * @param keys the key or keys to remove
	 */
	public void remove(Collection<String> keys) {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		for (String key : keys) {
			_remove(key);
		}
		serialized = null; // since the data changed, clear this so it's rebuilt
	}

	// the remove delegate
	private void _remove(String key) {
		// if the values had a key, then the data length had a length
		if (valuesMap.remove(key) != null) {
			dataLength -= lengthMap.remove(key);
		}
	}

	/**
	 * Returns the number of keys (case-sensitive) in the header.
	 *
	 * @return the number of header entries
	 */
	public int size() {
		return valuesMap.size();
	}

	/**
	 * Returns ture if map contains no keys.
	 *
	 * @return true if there are no headers
	 */
	public boolean isEmpty() {
		return valuesMap.isEmpty();
	}

	/**
	 * Removes all the keys The object map will be empty after this call returns.
	 */
	public void clear() {
		if (isReadOnly()) {
			throw new UnsupportedOperationException();
		}
		valuesMap.clear();
		lengthMap.clear();
		dataLength = 0;
		serialized = null;
	}

	/**
	 * Returns true if key (case-sensitive) is present (has values)
	 *
	 * @param key key whose presence is to be tested
	 * @return true if the key (case-sensitive) is present (has values)
	 */
	public boolean containsKey(String key) {
		return valuesMap.containsKey(key);
	}

	/**
	 * Returns true if key (case-insensitive) is present (has values)
	 *
	 * @param key exact key whose presence is to be tested
	 * @return true if the key (case-insensitive) is present (has values)
	 */
	public boolean containsKeyIgnoreCase(String key) {
		for (String k : valuesMap.keySet()) {
			if (k.equalsIgnoreCase(key)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns a {@link Set} view of the keys (case-sensitive) contained in the object.
	 *
	 * @return a read-only set the keys contained in this map
	 */
	public Set<String> keySet() {
		return Collections.unmodifiableSet(valuesMap.keySet());
	}

	/**
	 * Returns a {@link Set} view of the keys (case-insensitive) contained in the object.
	 *
	 * @return a read-only set of keys (in lowercase) contained in this map
	 */
	public Set<String> keySetIgnoreCase() {
		HashSet<String> set = new HashSet<>(valuesMap.size()*4/3 + 1);
		for (String k : valuesMap.keySet()) {
			set.add(k.toLowerCase());
		}
		return Collections.unmodifiableSet(set);
	}

	/**
	 * Returns a {@link List} view of the values for the specific (case-sensitive) key.
	 * Will be {@code null} if the key is not found.
	 *
	 * @param key the key whose associated value is to be returned
	 * @return a read-only list of the values for the case-sensitive key.
	 */
	public @Nullable List<String> get(String key) {
		List<String> values = valuesMap.get(key);
		return values == null ? null : Collections.unmodifiableList(values);
	}

	/**
	 * Returns the first value for the specific (case-sensitive) key.
	 * Will be {@code null} if the key is not found.
	 * @param key the key whose associated value is to be returned
	 * @return the first value for the case-sensitive key.
	 */
	public @Nullable String getFirst(String key) {
		List<String> values = valuesMap.get(key);
		return values == null ? null : values.get(0);
	}

	/**
	 * Returns the last value for the specific (case-sensitive) key.
	 * Will be {@code null} if the key is not found.
	 *
	 * @param key the key whose associated value is to be returned
	 * @return the last value for the case-sensitive key.
	 */
	public @Nullable String getLast(String key) {
		List<String> values = valuesMap.get(key);
		return values == null ? null : values.get(values.size() - 1);
	}

	/**
	 * Returns a {@link List} view of the values for the specific (case-insensitive) key.
	 * Will be {@code null} if the key is not found.
	 *
	 * @param key the key whose associated value is to be returned
	 * @return a read-only list of the values for the case-insensitive key.
	 */
	public @Nullable List<String> getIgnoreCase(String key) {
		List<String> values = new ArrayList<>();
		for (Map.Entry<String, List<String>> entry : valuesMap.entrySet()) {
			if (entry.getKey().equalsIgnoreCase(key)) {
				values.addAll(entry.getValue());
			}
		}
		return values.isEmpty() ? null : Collections.unmodifiableList(values);
	}

	/**
	 * Performs the given action for each header entry (case-sensitive keys) until all entries
	 * have been processed or the action throws an exception.
	 * Any attempt to modify the values will throw an exception.
	 *
	 * @param action The action to be performed for each entry
	 * @throws NullPointerException if the specified action is null
	 * @throws ConcurrentModificationException if an entry is found to be
	 * removed during iteration
	 */
	public void forEach(BiConsumer<String, List<String>> action) {
		valuesMap.forEach((key, values) ->
				action.accept(key, Collections.unmodifiableList(values)));
	}

	/**
	 * Returns a {@link Set} read only view of the mappings contained in the header (case-sensitive keys).
	 * The set is not modifiable and any attempt to modify will throw an exception.
	 *
	 * @return a set view of the mappings contained in this map
	 */
	public Set<Map.Entry<String, List<String>>> entrySet() {
		return Collections.unmodifiableSet(valuesMap.entrySet());
	}

	/**
	 * Returns if the headers are dirty, which means the serialization
	 * has not been done so also don't know the byte length
	 *
	 * @return true if dirty
	 */
	public boolean isDirty() {
		return serialized == null;
	}

	/**
	 * Returns the number of bytes that will be in the serialized version.
	 *
	 * @return the number of bytes
	 */
	public int serializedLength() {
		return dataLength + NON_DATA_BYTES;
	}

	private static final int HVCRLF_BYTES = HEADER_VERSION_BYTES_PLUS_CRLF.length;
	private static final int NON_DATA_BYTES = HVCRLF_BYTES + 2;

	/**
	 * Returns the serialized bytes.
	 *
	 * @return the bytes
	 */
	public byte[] getSerialized() {
		if (serialized == null) {
			serialized = new byte[serializedLength()];
			serializeToArray(0, serialized);
		}
		return serialized;
	}

	/**
	 * @deprecated
	 * Used for unit testing.
     * Appends the serialized bytes to the builder. 
     * 
	 * @param bab the ByteArrayBuilder to append
	 * @return the builder
	 */
	@Deprecated
	public ByteArrayBuilder appendSerialized(ByteArrayBuilder bab) {
		bab.append(HEADER_VERSION_BYTES_PLUS_CRLF);
		for (Map.Entry<String, List<String>> entry : valuesMap.entrySet()) {
			for (String value : entry.getValue()) {
				bab.append(entry.getKey());
				bab.append(COLON_BYTES);
				bab.append(value);
				bab.append(CRLF_BYTES);
			}
		}
		bab.append(CRLF_BYTES);
		return bab;
	}

	/**
	 * Write the header to the byte array. Assumes that the caller has
	 * already validated that the destination array is large enough by using getSerialized()
	 * @param destPosition the position index in destination byte array to start
	 * @param dest the byte array to write to
	 * @return the length of the header
	 */
	public int serializeToArray(int destPosition, byte[] dest) {
		System.arraycopy(HEADER_VERSION_BYTES_PLUS_CRLF, 0, dest, destPosition, HVCRLF_BYTES);
		destPosition += HVCRLF_BYTES;

		for (Map.Entry<String, List<String>> entry : valuesMap.entrySet()) {
			List<String> values = entry.getValue();
			for (String value : values) {
				byte[] bytes = entry.getKey().getBytes(US_ASCII);
				System.arraycopy(bytes, 0, dest, destPosition, bytes.length);
				destPosition += bytes.length;

				dest[destPosition++] = COLON;

				bytes = value.getBytes(US_ASCII);
				System.arraycopy(bytes, 0, dest, destPosition, bytes.length);
				destPosition += bytes.length;

				dest[destPosition++] = CR;
				dest[destPosition++] = LF;
			}
		}
		dest[destPosition++] = CR;
		dest[destPosition] = LF;

		return serializedLength();
	}

	/**
	 * Check the key to ensure it matches the specification for keys.
	 *
	 * @throws IllegalArgumentException if the key is null, empty or contains
	 *         an invalid character
	 */
	private void checkKey(String key) {
		// key cannot be null or empty and contain only printable characters except colon
		if (key == null || key.isEmpty()) {
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
	 * Check a non-null value if it matches the specification for values.
	 *
	 * @throws IllegalArgumentException if the value contains an invalid character
	 */
	private void checkValue(String val) {
		// Like rfc822 section 3.1.2 (quoted in ADR 4)
		// The field-body may be composed of any US-ASCII characters, except CR or LF.
		for (int i = 0, len = val.length(); i < len; i++) {
			int c = val.charAt(i);
			if (c > 127 || c == 10 || c == 13) {
				throw new IllegalArgumentException(VALUE_INVALID_CHARACTERS + Integer.toHexString(c));
			}
		}
	}

	private class Checker {
		List<String> list = new ArrayList<>();
		int len = 0;

		Checker(String key, Collection<String> values) {
			checkKey(key);
			if (!values.isEmpty()) {
				for (String val : values) {
					if (val != null) {
						if (val.isEmpty()) {
							list.add(val);
							len += key.length() + 3; // for colon, cr, lf
						}
						else {
							checkValue(val);
							list.add(val);
							len += key.length() + val.length() + 3; // for colon, cr, lf
						}
					}
				}
			}
		}

		boolean hasValues() {
			return !list.isEmpty();
		}
	}

	/**
	 * Whether the entire Headers is read only
	 * @return the read only state
	 */
	public boolean isReadOnly() {
		return !(valuesMap instanceof HashMap);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (!(o instanceof Headers)) return false;
		Headers headers = (Headers) o;
		return Objects.equals(valuesMap, headers.valuesMap);
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(valuesMap);
	}

	@Override
	public String toString() {
		return valuesMap.entrySet().stream()
				.filter(e -> e.getValue() != null)
				.sorted(Map.Entry.comparingByKey())
				.map(e -> {
					String headerName = e.getKey();
					List<String> values = e.getValue();
					return headerName +": "+ (values.size() == 1 ? values.get(0)
							: String.join(", ", values)
					);
				})
				.collect(Collectors.joining("; "));
	}
}
