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

public class Header {
    private static final String KEY_CANNOT_BE_EMPTY_OR_NULL = "Header key cannot be null.";
    private static final String VALUES_CANNOT_BE_EMPTY_OR_NULL = "Header values cannot be empty or null.";

    private String key;
    private Set<String> values;

    public Header(String key, String... values) {
        initKey(key);
        setValues(values);
    }

    public Header(String key, Collection<String> values) {
        initKey(key);
        setValues(values);
    }

    // key can only be set in the constructor
    private void initKey(String key) {
        if (key == null || key.length() == 0) {
            throw new IllegalArgumentException(KEY_CANNOT_BE_EMPTY_OR_NULL);
        }
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public Collection<String> getValues() {
        return Collections.unmodifiableCollection(values);
    }

    public Header values(String... values) {
        setValues(values);
        return this;
    }

    public Header values(Collection<String> values) {
        setValues(values);
        return this;
    }

    public void setValues(String... values) {
        valuesCannotBeEmptyOrNull(values);
        this.values = new HashSet<>();
        for (String v : values) {
            valueCannotBeEmptyOrNull(v);
            this.values.add(v);
        }
    }

    public void setValues(Collection<String> values) {
        valuesCannotBeEmptyOrNull(values);
        this.values = new HashSet<>();
        for (String v : values) {
            valueCannotBeEmptyOrNull(v);
            this.values.add(v);
        }
    }

    public boolean add(String... values) {
        valuesCannotBeEmptyOrNull(values);
        boolean changed = false;
        for (String v : values) {
            valueCannotBeEmptyOrNull(v);
            changed = this.values.add(v) || changed; // order is important!
        }
        return changed;
    }

    public boolean add(Collection<String> values) {
        valuesCannotBeEmptyOrNull(values);
        boolean changed = false;
        for (String v : values) {
            valueCannotBeEmptyOrNull(v);
            changed = this.values.add(v) || changed; // order is important!
        }
        return changed;
    }

    public boolean remove(String... values) {
        return remove(Arrays.asList(values));
    }

    public boolean remove(Collection<String> values) {
        boolean changed = this.values.removeAll(values);
        if (changed && this.values.size() == 0) {
            throw new IllegalArgumentException(VALUES_CANNOT_BE_EMPTY_OR_NULL);
        }
        return changed;
    }

    public int size() {
        return values.size();
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Header that = (Header) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(values, that.values);
    }

    public int hashCode() {
        return Objects.hash(key, values);
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

    private void valuesCannotBeEmptyOrNull(String[] vals) {
        if (vals == null || vals.length == 0) {
            throw new IllegalArgumentException(VALUES_CANNOT_BE_EMPTY_OR_NULL);
        }
    }
}
