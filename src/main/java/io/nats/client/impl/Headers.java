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
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Headers implements Iterable<Header> {
	private final Map<String, Header> headerMap = new HashMap<>();

	public Headers add(Header header) {
		return add(header.getKey(), header.getValues());
	}

	public Headers add(String key, String... values) {
		return add(key, Arrays.asList(values));
	}

	public Headers add(String key, Collection<String> values) {
		Header headerForKey = headerMap.get(key);
		if (headerForKey == null) {
			headerMap.put(key, new Header(key, values));
		}
		else {
			headerForKey.add(values);
		}
		return this;
	}

	public Headers set(Header header) {
		return set(header.getKey(), header.getValues());
	}

	public Headers set(String key, String... values) {
		return set(key, Arrays.asList(values));
	}

	public Headers set(String key, Collection<String> values) {
		headerMap.put(key, new Header(key, values));
		return this;
	}

	public boolean remove(String... keys) {
		return remove(Arrays.asList(keys));
	}

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

	public Header header(String key) {
		return headerMap.get(key);
	}

	public Collection<String> headerValues(String key) {
		Header header = headerMap.get(key);
		return header == null ? null : header.getValues();
	}

	public Set<String> keys() {
		return headerMap.keySet();
	}

	public Collection<Header> headers() {
		return Collections.unmodifiableCollection(headerMap.values());
	}

	public Stream<Header> stream() {
		return headerMap.values().stream();
	}

	@Override
	public Iterator<Header> iterator() {
		return headerMap.values().iterator();
	}

	@Override
	public void forEach(Consumer<? super Header> action) {
		headerMap.values().forEach(action);
	}

	@Override
	public Spliterator<Header> spliterator() {
		return headerMap.values().spliterator();
	}
}
