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
 * PublishAck objects represent a JetStream enabled server acknowledgment from a publish call.
 */
public interface PublishAck {

    /**
     * Get the stream sequence number for the corresponding published message.
     * @return the sequence number for the stored message.
     */
    long getSeqno();
    
    /**
     * Get the name of the stream a published message was stored in.
     * @return the the name of the stream.
     */
    String getStream();

    /**
     * Gets if the server detected the published message was a duplicate.
     * @return true if the message is a duplicate, false otherwise.
     */
    boolean isDuplicate();
}
