// Copyright 2022 The NATS Authors
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

public enum ObjectStoreWatchOption {
    /**
     * Do not include deletes or purges in results.
     * Default is to include deletes.
     */
    IGNORE_DELETE,

    /**
     * Watch starting at the first entry for all keys.
     * Default is to start at the last per key.
     */
    INCLUDE_HISTORY,

    /**
     * Watch starting when there are new entries for keys.
     * Default is to start at the last per key.
     */
    UPDATES_ONLY
}
