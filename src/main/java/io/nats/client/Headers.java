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

package io.nats.client;

import io.nats.client.impl.Header;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class Headers implements Iterable<Header> {
	private Map<String, Header> headerMap = new HashMap<>();

	public Headers add(Header header) {
		Header headerForKey = headerMap.get(header.getKey());
		if (headerForKey == null) {
			headerMap.put(header.getKey(), header);
		}
		else {
			headerForKey.add(header.getValues());
		}
		return this;
	}

	public Headers add(String key, String... values) {
		Header headerForKey = headerMap.get(key);
		if (headerForKey == null) {
			headerMap.put(key, new Header(key, values));
		}
		else {
			headerForKey.add(values);
		}
		return this;
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

	public Headers add(Headers headers) {
		headers.forEach(this::add);
		return this;
	}

	public Headers add(Map<String, Collection<String>> map) {
		map.forEach(this::add);
		return this;
	}

	public int size() {
		return headerMap.size();
	}

	public boolean isEmpty() {
		return headerMap.isEmpty();
	}

	public boolean containsKey(String key) {
		return headerMap.containsKey(key);
	}

	public Header get(String key) {
		return headerMap.get(key);
	}

	public Collection<String> getValues(String key) {
		Header header = headerMap.get(key);
		return header == null ? null : header.getValues();
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

	public Collection<Header> getHeaders() {
		return headerMap == null || headerMap.isEmpty()
				? Collections.emptyList()
				: Collections.unmodifiableCollection(headerMap.values());
	}
}
