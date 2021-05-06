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

package io.nats.examples.jsmulti;

interface Constants {
    String PUB_ACTIONS = "pubsync pubasync pubcore";
    String SUB_ACTIONS = "subpush subqueue subpull subpullqueue";
    String ALL_ACTIONS = PUB_ACTIONS + " " + SUB_ACTIONS;
    String PULL_ACTIONS = "subpull subpullqueue";
    String QUEUE_ACTIONS = "subqueue subpullqueue";

    String PUB_SYNC = "pubsync";
    String PUB_ASYNC = "pubasync";
    String PUB_CORE = "pubcore";
    String SUB_PUSH = "subpush";
    String SUB_QUEUE = "subqueue";
    String SUB_PULL = "subpull";
    String SUB_PULL_QUEUE = "subpullqueue";

    String INDIVIDUAL = "individual";
    String SHARED = "shared";
    String ITERATE = "iterate";
    String FETCH = "fetch";

    String NA = "N/A";
}
