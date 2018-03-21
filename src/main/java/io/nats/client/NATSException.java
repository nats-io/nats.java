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

package io.nats.client;

public class NATSException extends Exception {
    private static final long serialVersionUID = 1L;

    private Connection nc;
    private Subscription sub;

    NATSException() {
        super();
    }

    public NATSException(String msg) {
        super(msg);
    }

    NATSException(Throwable ex) {
        super(ex);
    }

    public NATSException(String string, Exception ex) {
        super(string, ex);
    }

    /**
     * General-purpose NATS exception for asynchronous events.
     *
     * @param ex  the asynchronous exception
     * @param nc  the NATS connection on which the event occurred (if applicable)
     * @param sub the {@code Subscription} on which the event occurred (if applicable)
     */
    public NATSException(Throwable ex, Connection nc, Subscription sub) {
        super(ex);
        this.setConnection(nc);
        this.setSubscription(sub);
    }

    public void setConnection(Connection nc) {
        this.nc = nc;
    }

    public Connection getConnection() {
        return this.nc;
    }

    public void setSubscription(Subscription sub) {
        this.sub = sub;
    }

    public Subscription getSubscription() {
        return sub;
    }
}
