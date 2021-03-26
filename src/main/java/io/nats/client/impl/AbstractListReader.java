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

import io.nats.client.JetStreamApiException;
import io.nats.client.Message;

import java.util.List;

abstract class AbstractListReader {

    enum ListType {OBJECTS, STRINGS}

    private final String objectName;
    private final ListType listType;
    private final String filterFieldName;
    protected ListRequestEngine engine;

    void process(Message msg) throws JetStreamApiException {
        engine = new ListRequestEngine(msg);
        List<String> list = listType == ListType.OBJECTS
                ? engine.getObjectList(objectName)
                : engine.getStringList(objectName);
        processItems(list);
    }

    abstract void processItems(List<String> items);

    AbstractListReader(String objectName, ListType listType) {
        this(objectName, listType, null);
    }

    AbstractListReader(String objectName, ListType listType, String filterFieldName) {
        this.objectName = objectName;
        this.listType = listType;
        this.filterFieldName = filterFieldName;
        engine = new ListRequestEngine();
    }

    byte[] nextJson() {
        return engine.internalNextJson();
    }

    byte[] nextJson(String filter) {
        if (filterFieldName == null) {
            throw new IllegalArgumentException("Filter not supported.");
        }
        return engine.internalNextJson(filterFieldName, filter);
    }

    boolean hasMore() {
        return engine.hasMore();
    }
}
