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

import static io.nats.client.utility.RetryConfig.DEFAULT_CONFIG;

public class Retrier {

    public static <T> T execute(RetryConfig config, RetryAction<T> action) throws Exception {
        return execute(config, action, e -> true);
    }

    public static <T> T execute(RetryConfig config, RetryAction<T> action, RetryObserver observer) throws Exception {
        int plen = config.backoffPolicy.length;
        int retries = 0;
        long deadlineExpiresAt = System.currentTimeMillis() + config.deadline;
        if (deadlineExpiresAt < System.currentTimeMillis()) {
            deadlineExpiresAt = Long.MAX_VALUE;
        }

        while (true) {
            try {
                return action.execute();
            }
            catch (Exception e) {
                if (++retries <= config.attempts && deadlineExpiresAt > System.currentTimeMillis() && observer.shouldRetry(e)) {
                    try {
                        int ix = retries - 1;
                        long sleep = ix < config.backoffPolicy.length ? config.backoffPolicy[ix] : config.backoffPolicy[plen-1];
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

    public static PublishAck publish(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) throws Exception {
        return execute(config,
            () -> js.publish(subject, headers, body, options),
            e -> e.getMessage().contains(Status.NO_RESPONDERS_TEXT));
    }

    public static PublishAck publish(RetryConfig config, JetStream js, String subject, byte[] body) throws Exception {
        return publish(config, js, subject, null, body, null);
    }

    public static PublishAck publish(RetryConfig config, JetStream js, String subject, Headers headers, byte[] body) throws Exception {
        return publish(config, js, subject, headers, body, null);
    }

    public static PublishAck publish(RetryConfig config, JetStream js, String subject, byte[] body, PublishOptions options) throws Exception {
        return publish(config, js, subject, null, body, options);
    }

    public static PublishAck publish(RetryConfig config, JetStream js, Message message) throws Exception {
        return publish(config, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    public static PublishAck publish(RetryConfig config, JetStream js, Message message, PublishOptions options) throws Exception {
        return publish(config, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }

    public static PublishAck publish(JetStream js, String subject, Headers headers, byte[] body, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, headers, body, options);
    }

    public static PublishAck publish(JetStream js, String subject, byte[] body) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, null, body, null);
    }

    public static PublishAck publish(JetStream js, String subject, Headers headers, byte[] body) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, headers, body, null);
    }

    public static PublishAck publish(JetStream js, String subject, byte[] body, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, subject, null, body, options);
    }

    public static PublishAck publish(JetStream js, Message message) throws Exception {
        return publish(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), null);
    }

    public static PublishAck publish(JetStream js, Message message, PublishOptions options) throws Exception {
        return publish(DEFAULT_CONFIG, js, message.getSubject(), message.getHeaders(), message.getData(), options);
    }
}
