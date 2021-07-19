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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class TestHandler implements ErrorListener, ConnectionListener {
    private final AtomicInteger count = new AtomicInteger();

    private final HashMap<Events,AtomicInteger> eventCounts = new HashMap<>();
    private final HashMap<String,AtomicInteger> errorCounts = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicInteger exceptionCount = new AtomicInteger();

    private CompletableFuture<Boolean> statusChanged;
    private CompletableFuture<Boolean> slowSubscriber;
    private Events eventToWaitFor;

    private Connection connection;
    private final ArrayList<Consumer> slowConsumers = new ArrayList<>();
    private final ArrayList<Message> discardedMessages = new ArrayList<>();

    private boolean printExceptions;

    public TestHandler() {
        this.printExceptions = true;
    }

    public TestHandler(boolean printExceptions) {
        this.printExceptions = printExceptions;
    }

    public void prepForStatusChange(Events waitFor) {
        lock.lock();
        try {
            statusChanged = new CompletableFuture<>();
            eventToWaitFor = waitFor;
        } finally {
            lock.unlock();
        }
    }

    public void waitForStatusChange(long timeout, TimeUnit units) {
        try {
            this.statusChanged.get(timeout, units);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            if (printExceptions) {
                e.printStackTrace();
            }
        }
    }

    public void exceptionOccurred(Connection conn, Exception exp) {
        this.connection = conn;
        this.count.incrementAndGet();
        this.exceptionCount.incrementAndGet();

        if( exp != null && this.printExceptions){
            System.out.println("Current time - "+System.currentTimeMillis());
            exp.printStackTrace();
            Statistics stats = conn.getStatistics();
            System.out.println("Sent "+stats.getOutMsgs()+"/"+stats.getOutBytes()+" - Received "+stats.getInMsgs()+"/"+stats.getInBytes());
        }
    }

    public void errorOccurred(Connection conn, String type) {
        this.connection = conn;
        this.count.incrementAndGet();

        lock.lock();
        try {
            AtomicInteger counter = errorCounts.get(type);
            if (counter == null) {
                counter = new AtomicInteger();
                errorCounts.put(type, counter);
            }
            counter.incrementAndGet();
        } finally {
            lock.unlock();
        }
    }

    public void messageDiscarded(Connection conn, Message msg) {
        this.connection = conn;
        this.count.incrementAndGet();

        lock.lock();
        try {
            this.discardedMessages.add(msg);
        } finally {
            lock.unlock();
        }
    }

    public void connectionEvent(Connection conn, Events type) {
        this.connection = conn;
        this.count.incrementAndGet();

        lock.lock();
        try {
            AtomicInteger counter = eventCounts.get(type);
            if (counter == null) {
                counter = new AtomicInteger();
                eventCounts.put(type, counter);
            }
            counter.incrementAndGet();

            System.out.println("Status change "+type);

            if (statusChanged != null && type == eventToWaitFor) {
                statusChanged.complete(Boolean.TRUE);
            }
            
        } finally {
            lock.unlock();
        }
    }

    public Future<Boolean> waitForSlow() {
        this.slowSubscriber = new CompletableFuture<>();
        return this.slowSubscriber;
    }

    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        this.count.incrementAndGet();

        lock.lock();
        try {
            this.slowConsumers.add(consumer);

            if (this.slowSubscriber != null) {
                this.slowSubscriber.complete(true);
            }
        } finally {
            lock.unlock();
        }
    }

    public List<Consumer> getSlowConsumers() {
        return this.slowConsumers;
    }

    public List<Message> getDiscardedMessages() {
        return this.discardedMessages;
    }

    public int getCount() {
        return this.count.get();
    }

    public int getExceptionCount() {
        return this.exceptionCount.get();
    }

    public int getEventCount(Events type) {
        int retVal = 0;
        lock.lock();
        try {
            AtomicInteger counter = eventCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            lock.unlock();
        }
        return retVal;
    }

    public int getErrorCount(String type) {
        int retVal = 0;
        lock.lock();
        try {
            AtomicInteger counter = errorCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            lock.unlock();
        }
        return retVal;
    }

    public void dumpErrorCountsToStdOut() {
        lock.lock();
        try {
            System.out.println("#### Test Handler Error Counts ####");
            for (String key : errorCounts.keySet()) {
                int count = errorCounts.get(key).get();
                System.out.println(key+": "+count);
            }
        } finally {
            lock.unlock();
        }
    }

    public Connection getConnection() {
        return this.connection;
    }

    public void setPrintExceptions(boolean tf) {
        this.printExceptions = tf;
    }
}