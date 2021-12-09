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
 * Key Value Watch Option
 */
public enum KeyValueWatchOption {
//    type watchOpts struct {
//        ctx context.Context
//        // Do not send delete markers to the update channel.
//        ignoreDeletes bool
//        // Include all history per subject, not just last one.
//        includeHistory bool
//        // For watches, skip the last entry. Does not apply to history
//        updatesOnly bool
//        // retrieve only the meta data of the entry
//        metaOnly bool
//    }

    /**
     * Do not send delete or purge markers as updates.
     */
    IGNORE_DELETES, META_ONLY;
}
