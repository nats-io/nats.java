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

public class Headers {
	private static final String KEY_CANNOT_BE_EMPTY_OR_NULL = "Header key cannot be null.";
	private static final String KEY_INVALID_CHARACTER = "Header key has invalid character: ";
	private static final String VALUE_INVALID_CHARACTERS = "Header value has invalid character: ";

	private final Map<String, List<String>> headerMap = new HashMap<>();
	private final Map<String, String> valueCsvCache = new HashMap<>();

	public String getValueCsv(String key) {
		// I made this a cache because
		return valueCsvCache.computeIfAbsent(key, k -> String.join(",", values(key)));
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
	public void add(String key, String... values) {
		if (values != null) {
			add(key, Arrays.asList(values));
		}
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
	public void add(String key, Collection<String> values) {
		if (values != null) {
			List<String> checked = checkKeyValues(key, values);
			if (!checked.isEmpty()) {
				List<String> currentSet = headerMap.get(key);
				if (currentSet == null) {
					headerMap.put(key, checked);
				} else {
					currentSet.addAll(checked);
				}
				valueCsvCache.remove(key); // remove from cache so it will be re-calculated
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
	public void put(String key, String... values) {
		if (values != null) {
			put(key, Arrays.asList(values));
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
	public void put(String key, Collection<String> values) {
		if (values != null) {
			List<String> checked = checkKeyValues(key, values);
			if (!checked.isEmpty()) {
				headerMap.put(key, checked);
				valueCsvCache.remove(key); // remove from cache so it will be re-calculated
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
			valueCsvCache.remove(key); // remove from cache so it will be re-calculated
		}
	}

	/**
	 * Removes each key and its values if the key was present
	 *
	 * @param keys the key or keys to remove
	 */
	public void remove(Collection<String> keys) {
		for (String key : keys) {
			headerMap.remove(key);
			valueCsvCache.remove(key); // remove from cache so it will be re-calculated
		}
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
		return headerMap.containsKey(key);
	}

	public List<String> values(String key) {
		List<String> list = headerMap.get(key);
		return list == null ? null : Collections.unmodifiableList(list);
	}

	public Set<String> keySet() {
		return headerMap.keySet();
	}

	private List<String> checkKeyValues(String key, Collection<String> values) {
		checkKey(key);

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

	private void checkKey(String key) {
		// key cannot be null or empty and contain only printable characters except colon
		if (key == null || key.length() == 0) {
			throw new IllegalArgumentException(KEY_CANNOT_BE_EMPTY_OR_NULL);
		}

		key.chars().forEach(c -> {
			if (c < 32 || c > 126 || c == ':') {
				throw new IllegalArgumentException(KEY_INVALID_CHARACTER + c);
			}
		});
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
