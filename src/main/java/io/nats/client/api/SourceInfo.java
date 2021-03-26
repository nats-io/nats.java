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

package io.nats.client.api;

import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.ApiConstants.SOURCE;
import static io.nats.client.support.ApiConstants.SOURCES;
import static io.nats.client.support.JsonUtils.getObjectList;

/**
 * Information about a stream being sourced
 */
public class SourceInfo extends SourceInfoBase {

    static List<SourceInfo> optionalListOf(String json) {
        List<String> strObjects = getObjectList(SOURCES, json);
        List<SourceInfo> list = new ArrayList<>();
        for (String j : strObjects) {
            list.add(new SourceInfo(j));
        }
        return list.isEmpty() ? null : list;
    }

    SourceInfo(String json) {
        super(json, SOURCE);
    }
}
