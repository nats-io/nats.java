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

/**
 * SIMPLIFICATION IS EXPERIMENTAL AND SUBJECT TO CHANGE
 */
public interface ConsumerContext {
    String getName();
    ConsumerInfo getConsumerInfo() throws IOException, JetStreamApiException;
    FetchConsumer fetch(int maxMessages) throws IOException, JetStreamApiException;
    FetchConsumer fetch(int maxBytes, int maxMessages) throws IOException, JetStreamApiException;
    FetchConsumer fetch(FetchConsumeOptions consumeOptions) throws IOException, JetStreamApiException;
    ManualConsumer consume() throws IOException, JetStreamApiException;
    ManualConsumer consume(ConsumeOptions consumeOptions) throws IOException, JetStreamApiException;
    SimpleConsumer consume(MessageHandler handler) throws IOException, JetStreamApiException;
    SimpleConsumer consume(MessageHandler handler, ConsumeOptions consumeOptions) throws IOException, JetStreamApiException;
}
