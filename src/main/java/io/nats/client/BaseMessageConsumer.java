// Copyright 2020-2023 The NATS Authors
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

/**
 * The BaseMessageConsumer interface is the core interface replacing
 * a subscription for a simplified consumer.
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public interface BaseMessageConsumer extends AutoCloseable {
    /**
     * Stop the MessageConsumer from asking for any more messages from the server.
     * The consumer will finish all pull request already in progress, but will not start any new ones.
     */
    void stop();
}
