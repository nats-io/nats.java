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

package io.nats.client.jetstream;

/**
 * Publish expectation lets the server know expectations to avoid unnecessarily
 * persisting messages in the server.
 */
public interface PublishExpectation {

    /**
     * Informs the server the publisher is expecting this message to be persisted
     * into a specific stream.  If the subject is not part of the stream, the
     * message is rejected.
     *
     * @param stream expected stream name
     * @return a publish expectation
     */
    PublishExpectation stream(String stream);

    /**
     * Informs the server the publisher is expecting this messages to be persisted
     * into a specific stream.  If the subject is not part of the stream, the
     * message is rejected.
     *
     * @param sequence expected sequence number
     * @return a publish expectation
     */
    PublishExpectation seqence(long sequence);
}
