// Copyright 2024 The NATS Authors
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

package io.nats.client.utility;

import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.impl.Headers;
import io.nats.client.support.Status;

import java.util.concurrent.CompletableFuture;

import static io.nats.client.utility.RetryConfig.DEFAULT_CONFIG;

/**
 * The Retrier is designed to give generic retry ability to retry anything.
 * There are also static methods which are use the generic ability that are built specifically for JetStream publishing.
 */
public class Retrier {

    /**
     * Execute the supplied action with the given retry config.
     * @param config The custom retry config
     * @param action The retry action
     * @return an instance of the return type
     * @param <T> the return type
     * @throws Exception various execution exceptions; only thrown if all retries failed.
     */
    public static <T> T execute(RetryConfig config, RetryAction<T> action) throws Exception {
        return execute(config, action, e -> true);
    }

    /**
     * Execute the supplied action with the given retry config.
     * @param config The custom retry config
     * @param action The retry action
     * @param observer The retry observer
     * @return an instance of the return type
     * @param <T> the return type
     * @throws Exception various execution exceptions; only thrown if all retries failed
     * or the observer declines to retry.
     */
    public static <T> T execute(RetryConfig config, RetryAction<T> action, RetryObserver observer) throws Exception {
        long[] backoffPolicy = config.getBackoffPolicy();;
        int plen = backoffPolicy.length;
        int retries = 0;
        long deadlineExpiresAt = System.currentTimeMillis() + config.getDeadline();
        if (deadlineExpiresAt < System.currentTimeMillis()) {
            deadlineExpiresAt = Long.MAX_VALUE;
        }

        while (true) {
            try {
                return action.execute();
            }
            catch (Exception e) {
                if (++retries <= config.getAttempts() && deadlineExpiresAt > System.currentTimeMillis() && observer.shouldRetry(e)) {
                    try {
                        int ix = retries - 1;
                        long sleep = ix < backoffPolicy.length ? backoffPolicy[ix] : backoffPolicy[plen-1];
                        //noinspection BusyWait
                        Thread.sleep(sleep);
                        continue; // goes back to start of while
                    }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
                throw e;
            }
        }
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) throws Exception {
        return execute(config,
            () -> js.publish(subject, headers, body, options),
            e -> e.getMessage().contains(Status.NO_RESPONDERS_TEXT));
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, byte[] body) throws Exception {
        return publish(config, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body) throws Exception {
        return publish(config, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, String subject, byte[] body, PublishOptions options) throws Exception {
        return publish(config, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, Message message) throws Exception {
        return publish(config, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(RetryConfig config, JetStream js, Message message, PublishOptions options) throws Exception {
        return publish(config, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, headers, body, options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, byte[] body) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, Headers headers, byte[] body) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, String subject, byte[] body, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the default retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, Message message) throws Exception {
        return publish(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject and waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The acknowledgement of the publish
     * @throws Exception various communication issues with the NATS server; only thrown if all retries failed.
     */
    public static PublishAck publish(JetStream js, Message message, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return publish(config, js, subject, headers, body, options);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, byte[] body) {
        return publishAsync(config, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body) {
        return publishAsync(config, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, String subject, byte[] body, PublishOptions options) {
        return publishAsync(config, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, Message message) {
        return publishAsync(config, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param config The custom retry config
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(RetryConfig config, JetStream js, Message message, PublishOptions options) {
        return publishAsync(config, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) {
        return publishAsync(DEFAULT_CONFIG, js, subject, headers, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, byte[] body) {
        return publishAsync(DEFAULT_CONFIG, js, subject, null, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param headers optional headers to publish with the message.
     * @param body the message body
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, Headers headers, byte[] body) {
        return publishAsync(DEFAULT_CONFIG, js, subject, headers, body, null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param subject the subject to send the message to
     * @param body the message body
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, String subject, byte[] body, PublishOptions options) {
        return publishAsync(DEFAULT_CONFIG, js, subject, null, body, options);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, Message message) {
        return publishAsync(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    /**
     * Send a message to the specified subject, executing in a future. Waits for a response from Jetstream,
     * retrying until the ack is received or the retry config is exhausted.
     * @param js the JetStream context
     * @param message the message to publish
     * @param options publish options
     * @return The future
     */
    public static CompletableFuture<PublishAck> publishAsync(JetStream js, Message message, PublishOptions options) {
        return publishAsync(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }
}
