// Copyright 2021 The NATS Authors
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

/**
 * Use the KeyValueWatcher interface to watch for updates
 */
public interface KeyValueWatcher {

    /**
     * Called when a key has been updated
     *
     * @param kve The entry for the updated key
     */
    void watch(KeyValueEntry kve);

    /**
     * Called once if there is no data when the watch is created
     * or if there is data, the first time the watch exhausts all existing data.
     */
    void endOfData();
}
