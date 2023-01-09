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

import io.nats.client.support.JsonValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Information about a stream being sourced
 */
public class SourceInfo extends SourceInfoBase {

    static List<SourceInfo> optionalListOf(List<JsonValue> arrayOfSource) {
        if (arrayOfSource == null) {
            return null;
        }
        List<SourceInfo> list = new ArrayList<>();
        for (JsonValue jv : arrayOfSource) {
            list.add(new SourceInfo(jv));
        }
        return list;
    }

    SourceInfo(JsonValue vSourceInfo) {
        super(vSourceInfo);
    }
}
