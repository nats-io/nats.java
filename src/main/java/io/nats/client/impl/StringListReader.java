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

import io.nats.client.support.JsonValue;

import java.util.ArrayList;
import java.util.List;

abstract class StringListReader extends AbstractListReader {

    List<String> strings;

    StringListReader(String objectName, String filterFieldName) {
        super(objectName, filterFieldName);
        strings = new ArrayList<>();
    }

    @Override
    void processItems(List<JsonValue> items) {
        for (JsonValue v : items) {
            if (v.string != null) {
                strings.add(v.string);
            }
        }
    }

    List<String> getStrings() {
        return strings;
    }
}
