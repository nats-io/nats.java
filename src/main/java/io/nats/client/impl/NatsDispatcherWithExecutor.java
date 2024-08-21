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

package io.nats.client.impl;

import io.nats.client.MessageHandler;

class NatsDispatcherWithExecutor extends NatsDispatcher {

    NatsDispatcherWithExecutor(NatsConnection conn, MessageHandler handler) {
        super(conn, handler);
    }

    @Override
    public void run() {
        try {
            while (this.running.get() && !Thread.interrupted()) {
                NatsMessage msg = this.incoming.pop(this.waitForMessage);
                if (msg != null) {
                    NatsSubscription sub = msg.getNatsSubscription();
                    if (sub != null && sub.isActive()) {
                        MessageHandler handler = subscriptionHandlers.get(sub.getSID());
                        if (handler == null) {
                            handler = defaultHandler;
                        }
                        // A dispatcher can have a null defaultHandler. You can't subscribe without a handler,
                        // but messages might come in while the dispatcher is being closed or after unsubscribe
                        // and the [non-default] handler has already been removed from subscriptionHandlers
                        if (handler != null) {
                            sub.incrementDeliveredCount();
                            this.incrementDeliveredCount();

                            MessageHandler finalHandler = handler;
                            connection.getExecutor().execute(() -> {
                                try {
                                    finalHandler.onMessage(msg);
                                } catch (Exception exp) {
                                    connection.processException(exp);
                                } catch (Error err) {
                                    connection.processException(new Exception(err));
                                }

                                if (sub.reachedUnsubLimit()) {
                                    connection.invalidate(sub);
                                }
                            });
                        }
                    }
                }

                if (breakRunLoop()) {
                    return;
                }
            }
        }
        catch (InterruptedException exp) {
            if (this.running.get()){
                this.connection.processException(exp);
            } //otherwise we did it
            Thread.currentThread().interrupt();
        }
        finally {
            this.running.set(false);
            this.thread = null;
        }
    }
}
