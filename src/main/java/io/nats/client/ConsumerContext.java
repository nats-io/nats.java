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

import io.nats.client.api.ConsumerInfo;

import java.io.IOException;
import java.time.Duration;

/**
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public interface ConsumerContext {
    String getConsumerName();
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;
    Message next() throws IOException, InterruptedException, JetStreamStatusCheckedException;
    Message next(Duration maxWait) throws IOException, InterruptedException, JetStreamStatusCheckedException;
    Message next(long maxWaitMillis) throws IOException, InterruptedException, JetStreamStatusCheckedException;
    FetchConsumer fetchMessages(int maxMessages);
    FetchConsumer fetchBytes(int maxBytes);
    FetchConsumer fetch(FetchConsumeOptions consumeOptions);
    ManualConsumer consume();
    ManualConsumer consume(ConsumeOptions consumeOptions);
    SimpleConsumer consume(MessageHandler handler);
    SimpleConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions);
}
