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
	private static final String VALUES_CANNOT_BE_EMPTY_OR_NULL = "Header values cannot be empty or null.";

	private final Map<String, Set<String>> headerMap = new HashMap<>();

	/**
	 * If the key is present add the values to the set of values for the key.
	 * If the key is not present, sets the specified values for the key.
	 * Duplicate values are ignored. Null and empty values are not allowed
	 *
	 * @param key the key
	 * @param values the values
	 * @return {@code true} if this object did not already contain values for the key
	 * @throws IllegalArgumentException if the key is null or empty
	 *         -or- any value is null or empty.
	 */
	public boolean add(String key, String... values) {
		return add(key, Arrays.asList(values));
	}

	/**
	 * If the key is present add the values to the set of values for the key.
	 * If the key is not present, sets the specified values for the key.
	 * Duplicate values are ignored. Null and empty values are not allowed.
	 *
	 * @param key the key
	 * @param values the values
	 * @return {@code true} if this object did not already contain the key or the
	 *         values for the key changed.
	 * @throws IllegalArgumentException if the key is null or empty
	 *         -or- if then input collection is null
	 *         -or- if any item in the collection is null or empty.
	 */
	public boolean add(String key, Collection<String> values) {
		Set<String> validatedSet = validateKeyAndValues(key, values);

		Set<String> currentSet = headerMap.get(key);
		if (currentSet == null) {
			headerMap.put(key, validatedSet);
			return true;
		}

		return currentSet.addAll(validatedSet);
	}

	private Set<String> validateKeyAndValues(String key, Collection<String> values) {
		keyCannotBeNull(key);
		valuesCannotBeEmptyOrNull(values);
		Set<String> validatedSet = new HashSet<>();
		for (String v : values) {
			valueCannotBeEmptyOrNull(v);
			validatedSet.add(v);
		}
		return validatedSet;
	}

	/**
	 * Associates the specified values with the key. If the key was already present
	 * any existing values are removed and replaced with the new set.
	 * Duplicate values are ignored. Null and empty values are not allowed
	 *
	 * @param key the key
	 * @param values the values
	 * @return {@code true} if this object did not already contain values for the key
	 * @throws IllegalArgumentException if the key is null or empty
	 *         -or- any value is null or empty.
	 */
	public boolean put(String key, String... values) {
		return put(key, Arrays.asList(values));
	}

	/**
	 * Associates the specified values with the key. If the key was already present
	 * any existing values are removed and replaced with the new set.
	 * Duplicate values are ignored. Null and empty values are not allowed
	 *
	 * @param key the key
	 * @param values the values
	 * @return {@code true} if this object did not already contain values for the key
	 * @throws IllegalArgumentException if the key is null or empty
	 *         -or- if then input collection is null
	 *         -or- if any item in the collection is null or empty.
	 */
	public boolean put(String key, Collection<String> values) {
		Set<String> validatedSet = validateKeyAndValues(key, values);
		return headerMap.put(key, validatedSet) == null;
	}

	/**
	 * Removes each key and its values if the key was present
	 *
	 * @param keys the key or keys to remove
	 * @return {@code true} if any key was present
	 */
	public boolean remove(String... keys) {
		return remove(Arrays.asList(keys));
	}

	/**
	 * Removes each key and its values if the key was present
	 *
	 * @param keys the key or keys to remove
	 * @return {@code true} if any key was present
	 */
	public boolean remove(Collection<String> keys) {
		boolean changed = false;
		for (String key : keys) {
			if (headerMap.remove(key) != null) {
				changed = true;
			}
		}
		return changed;
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

	public Set<String> values(String key) {
		Set<String> set = headerMap.get(key);
		return set == null ? null : Collections.unmodifiableSet(set);
	}

	public Set<String> keySet() {
		return headerMap.keySet();
	}

	private void keyCannotBeNull(String key) {
		if (key == null || key.length() == 0) {
			throw new IllegalArgumentException(KEY_CANNOT_BE_EMPTY_OR_NULL);
		}
	}

	private void valueCannotBeEmptyOrNull(String val) {
		if (val == null || val.length() == 0) {
			throw new IllegalArgumentException(VALUES_CANNOT_BE_EMPTY_OR_NULL);
		}
	}

	private void valuesCannotBeEmptyOrNull(Collection<String> vals) {
		if (vals == null || vals.size() == 0) {
			throw new IllegalArgumentException(VALUES_CANNOT_BE_EMPTY_OR_NULL);
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
