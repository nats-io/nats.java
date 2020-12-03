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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.List;
import java.util.stream.Collectors;

import static io.nats.client.support.NatsConstants.*;
import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * An object that represents a map of keys to a list of values. It does not accept
 * null or invalid keys. It ignores null values, accepts empty string as a value
 * and rejects invalid values.
 *
 * THIS CLASS IS NOT THREAD SAFE
 */
public class Headers {

	private static final String KEY_CANNOT_BE_EMPTY_OR_NULL = "Header key cannot be null.";
	private static final String KEY_INVALID_CHARACTER = "Header key has invalid character: ";
	private static final String VALUE_INVALID_CHARACTERS = "Header value has invalid character: ";

	private final Map<ByteBuffer, List<ByteBuffer>> headerMap;
	private ByteBuffer serialized;

	public Headers() {
		headerMap = new HashMap<>();
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

	public Headers add(ByteBuffer key, ByteBuffer... values) {
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
			List<ByteBuffer> valueList = values
					.stream()
					.map(value -> (value != null) ? US_ASCII.encode(value) : null)
					.collect(Collectors.toList());
			_add((key != null) ? US_ASCII.encode(key) : null, valueList);
		}
	}

	// the add delegate
	private void _add(ByteBuffer key, Collection<ByteBuffer> values) {
		checkKey(key);
		Checker checked = new Checker(key, values);
		if (checked.hasValues()) {
			List<ByteBuffer> currentSet = headerMap.get(key);
			if (currentSet == null) {
				headerMap.put(key, checked.list);
			} else {
				currentSet.addAll(checked.list);
			}
		}
		serialized = null; // since the data changed, clear this so it's rebuilt
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
			List<ByteBuffer> valueList = Arrays.asList(values)
					.stream()
					.map(value -> (value != null) ? US_ASCII.encode(value) : null)
					.collect(Collectors.toList());
			_put((key != null) ? US_ASCII.encode(key) : null, valueList);
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
		if (values != null) {
			List<ByteBuffer> valueList = values
					.stream()
					.map(value -> (value != null) ? US_ASCII.encode(value) : null)
					.collect(Collectors.toList());
			_put((key != null) ? US_ASCII.encode(key) : null, valueList);
		}
		return this;
	}

	// the put delegate that all puts call
	private void _put(ByteBuffer key, Collection<ByteBuffer> values) {
		if (values != null) {
			Checker checked = new Checker(key, values);
			checkKey(key);
			if (checked.hasValues()) {
				headerMap.put(key, checked.list);
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
			headerMap.remove(US_ASCII.encode(key));
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
			headerMap.remove(US_ASCII.encode(key));
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
		serialized = null;
	}

	/**
	 * Returns <tt>true</tt> if key is present (has values)
	 *
	 * @param key key whose presence is to be tested
	 * @return <tt>true</tt> if the key is present (has values)
	 */
	public boolean containsKey(String key) {
		return headerMap.containsKey(US_ASCII.encode(key));
	}

	/**
	 * Returns a {@link Set} view of the keys contained in the object.
	 *
	 * @return a read-only set the keys contained in this map
	 */
	public Set<String> keySet() {
		return headerMap.keySet()
				.stream()
				.map(value -> US_ASCII.decode(value.asReadOnlyBuffer()).toString())
				.collect(Collectors.toSet());
	}

	/**
	 * Returns a {@link List} view of the values for the specific key.
	 * Will be {@code null} if the key is not found.
	 *
	 * @return a read-only list of the values for the specific keys.
	 */
	public List<String> values(String key) {
		ByteBuffer keyBuffer = US_ASCII.encode(key);
		List<ByteBuffer> bufferList = headerMap.get(keyBuffer);
		List<String> list = null;
		if (bufferList != null) {
			list = headerMap.get(keyBuffer)
					.stream()
					.map(value -> US_ASCII.decode(value.asReadOnlyBuffer()).toString())
					.collect(Collectors.toList());
			if (list.isEmpty())
				list.add(EMPTY);
		}
		return list == null ? null : Collections.unmodifiableList(list);
	}

	/**
	 * Performs the given action for each header entry until all entries
	 * have been processed or the action throws an exception.
	 * Any attempt to modify the values will throw an exception.
	 *
	 * @param action The action to be performed for each entry
	 * @throws NullPointerException if the specified action is null
	 * @throws ConcurrentModificationException if an entry is found to be
	 * removed during iteration
	 */
	public void forEach(BiConsumer<String, List<String>> action) {
		Collections.unmodifiableMap(entryMap()).forEach(action);
	}

	/**
	 * Returns a {@link Set} read only view of the mappings contained in the header.
	 * The set is not modifiable and any attempt to modify will throw an exception.
	 *
	 * @return a set view of the mappings contained in this map
	 */
	public Set<Map.Entry<String, List<String>>> entrySet() {
		return Collections.unmodifiableSet(entryMap().entrySet());
	}

	private Map<String, List<String>> entryMap() {
		return headerMap.entrySet()
				.stream()
				.map(entry -> new AbstractMap.SimpleEntry<>(
						US_ASCII.decode(entry.getKey().asReadOnlyBuffer()).toString(),
						entry.getValue().stream()
								.map(value -> US_ASCII.decode(value.asReadOnlyBuffer()).toString())
								.collect(Collectors.toList())
				))
				.collect(Collectors.toMap(
						Map.Entry::getKey,
						Map.Entry::getValue
				));
	}

	/**
	 * Returns the number of bytes that will be in the serialized version.
	 *
	 * @return the number of bytes
	 */
	public int serializedLength() {
		if (serialized != null)
			return serialized.remaining();
		int len = VERSION_BYTES_PLUS_CRLF_LEN + CRLF_BYTES_LEN;
		for (ByteBuffer key : headerMap.keySet()) {
			for (ByteBuffer value : headerMap.get(key)) {
				len += key.remaining() + value.remaining() + 3;
			}
		}
		return len;
	}

	public byte[] getSerialized() {
		if (serialized == null) {
			ByteBuffer bab = ByteBuffer.allocate(serializedLength());
			bab.put(VERSION_BYTES_PLUS_CRLF.asReadOnlyBuffer());
			for (ByteBuffer key : headerMap.keySet()) {
				for (ByteBuffer value : headerMap.get(key)) {
					bab.put(key.asReadOnlyBuffer());
					bab.put(COLON_BYTES);
					bab.put(value.asReadOnlyBuffer());
					bab.put(CRLF.asReadOnlyBuffer());
				}
			}
			bab.put(CRLF.asReadOnlyBuffer());
			bab.flip();
			serialized = bab;
		}
		return serialized.array();
	}

	/**
	 * Check the key to ensure it matches the specification for keys.
	 *
	 * @throws IllegalArgumentException if the key is null, empty or contains
	 *         an invalid character
	 */
	private void checkKey(ByteBuffer key) {
		// key cannot be null or empty and contain only printable characters except colon
		if (key == null || key.remaining() == 0) {
			throw new IllegalArgumentException(KEY_CANNOT_BE_EMPTY_OR_NULL);
		}

		ByteBuffer tmpKey = key.asReadOnlyBuffer();
		while (tmpKey.hasRemaining()) {
			byte c = tmpKey.get();
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
	private void checkValue(ByteBuffer val) {
		// Generally more permissive than HTTP.  Allow only printable
		// characters and include tab (0x9) to cover what's allowed
		// in quoted strings and comments.
		// null is just ignored
		ByteBuffer valTmp = val.asReadOnlyBuffer();
		while (valTmp.hasRemaining()) {
			byte c = valTmp.get();
			if ((c < 32 && c != 9) || c > 126) {
				throw new IllegalArgumentException(VALUE_INVALID_CHARACTERS + c);
			}
		}
	}

	private class Checker {
		List<ByteBuffer> list = new ArrayList<>();
		int length;

		Checker(ByteBuffer key, Collection<ByteBuffer> values) {
			checkKey(key);
			length += key.remaining() + 1; // for colon
			if (values != null && !values.isEmpty()) {
				for (ByteBuffer val : values) {
					if (val != null) {
						if (!val.hasRemaining()) {
							list.add(val);
						} else {
							checkValue(val);
							list.add(val);
							length += val.remaining();
						}
					}
				}
			}
		}

		boolean hasValues() {
			return list.size() > 0;
		}
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
