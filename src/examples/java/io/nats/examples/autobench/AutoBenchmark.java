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

package io.nats.examples.autobench;

import io.nats.client.Connection;
import io.nats.client.Options;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AutoBenchmark {
    private final String name;
    private final long messageSize;
    private final long messageCount;
    private final AtomicLong start;
    private long runtimeNanos;
    private Exception exception;

    public AutoBenchmark(String name, long messageCount, long messageSize) {
        this.name = name;
        this.messageCount = messageCount;
        this.messageSize = messageSize;
        this.start = new AtomicLong();
    }

    public abstract void execute(Options connectOptions) throws InterruptedException;

    public String getName() {
        return this.name;
    }

    public String getSubject() {
        return generate("sub");
    }

    public String getStream() {
        return generate("stream");
    }

    private String generate(String prefix) {
        return prefix + toSafeName(getName()) + "x" + getMessageCount();
    }

    public static String toSafeName(String text) {
        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < text.length(); x++) {
            char c = text.charAt(x);
            if (Character.isLetterOrDigit(c) || c == '.' || c == '-'|| c == '_') {
                sb.append(c);
            }
        }
        return sb.toString().trim();
    }

    public long getMessageSize() {
        return this.messageSize;
    }

    public long getMessageCount() {
        return this.messageCount;
    }

    public byte[] createPayload() {
        return new byte[(int)this.messageSize];
    }

    public long getStart() {
        return start.get();
    }

    public void startTiming() {
        start.set(System.nanoTime());
    }

    public void endTiming() {
        long end = System.nanoTime();
        this.runtimeNanos = end - start.get();
    }

    public void reset() {
        this.runtimeNanos = 0;
        this.exception = null;
    }

    public void defaultFlush(Connection nc) {
        try {
            nc.flush(Duration.ofSeconds(5));
        }
        catch(Exception e) { /* ignore */ }
    }

    public long getRuntimeNanos() {
        return this.runtimeNanos;
    }

    public void setException(Exception ex) {
        this.exception = ex;
    }

    public Exception getException() {
        return this.exception;
    }

    public void getFutureSafely(CompletableFuture<Void> future) {
        try {
            future.get();
        }
        catch(Exception e) { /* ignore */ }
    }

    public void beforePrintFirstOfKind() {}
    public void afterPrintLastOfKind() {}

    public void printResult() {
        if (this.runtimeNanos == 0) {
            System.out.printf("%-26s %18s\n", name, "no data from test run");
            return;
        } else if (this.exception != null) {
            String message = this.exception.getMessage();

            if (message == null) {
                message = this.exception.getClass().getCanonicalName();
            }

            System.out.printf("%-26s %12s Exception: %12s\n",
                    getName(),
                    NumberFormat.getIntegerInstance().format(this.messageCount),
                    message);
            return;
        }

        double messagesPerSecond = 1e9 * ((double)this.messageCount)/((double)this.runtimeNanos); // 1e9 for nanos to seconds
        double bytesPerSecond = 1e9 * ((double)(this.messageCount * this.messageSize))/((double)this.runtimeNanos);
        System.out.printf("%-26s %12s %18s msg/s %12s/s\n",
                            this.name,
                            NumberFormat.getIntegerInstance().format(this.messageCount),
                            NumberFormat.getIntegerInstance().format((long)messagesPerSecond),
                            AutoBenchmark.humanBytes(bytesPerSecond));
    }

    public static String humanBytes(double bytes) {
        int base = 1024;
        String[] pre = new String[] {"k", "m", "g", "t", "p", "e"};
        String post = "b";
        if (bytes < (long) base) {
            return String.format("%.2f b", bytes);
        }
        int exp = (int) (Math.log(bytes) / Math.log(base));
        int index = exp - 1;
        String units = pre[index] + post;
        return String.format("%.2f %s", bytes / Math.pow((double) base, (double) exp), units);
    }
}