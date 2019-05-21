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

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import io.nats.client.Connection;
import io.nats.client.Options;

public abstract class ThrottledBenchmark extends AutoBenchmark {

    private long targetPubRate = 0;
    private boolean pubFailed = false;
    private AtomicLong sent = new AtomicLong();

    public ThrottledBenchmark(String name, long messageCount, long messageSize) {
        super(name, messageCount, messageSize);
    }

    void pubFailed() {
        this.pubFailed = true;
    }

    /**
     * Sub-classes need to use adjust and sleep between every publish and 
     * call pubFailed() if the publisher fails.
     */
    abstract void executeWithLimiter(Options connectOptions) throws InterruptedException;

    public void execute(Options connectOptions) throws InterruptedException {
        while(true) {
            this.pubFailed = false;
            this.sent.set(0);
            this.reset();

            this.executeWithLimiter(connectOptions);

            if (this.getException() == null || pubFailed) {
                break;
            }

            // Keep going if sub failed, but slow down
            long currentPubRate = (long) ((1e9 * (double) this.sent.get()) / ((double)this.getRuntimeNanos()));

            if (this.targetPubRate == 0) {
                this.targetPubRate = 2 * currentPubRate;
            }

            this.targetPubRate = this.targetPubRate - this.targetPubRate/10;

            if (this.targetPubRate < 500) {
                break;
            }
        }

    }

    /**
     * Throttled benchmarks use an optional target send rate. This is to make sure the subscribers are not overrun,
     * become slow consumers. The addition of this code was motivated by the asymetry in the java library between
     * publishers and subscribers, especially if UTF-8 subjects are enabled.
     * @param nc the connection
     * @throws InterruptedException
     */
	void adjustAndSleep(Connection nc) throws InterruptedException {

        long count = sent.incrementAndGet();

        if (this.targetPubRate <= 0) {
            return;
        } else if (count % 1000 != 0) { // Only sleep every 1000 message
            return;
        }

        long now = System.nanoTime();
        long start = this.getStart();
        double rate = (1e9 * (double) count)/((double)(now - start));
        double delay = (1.0/((double)this.targetPubRate));
        double adjust = delay / 20.0; // 5%
        
		if (adjust == 0) {
			adjust = 1e-9; // 1ns min
        }
        
		if (rate < this.targetPubRate) {
			delay -= adjust;
		} else if (rate > this.targetPubRate) {
			delay += adjust;
        }
        
		if (delay < 0) {
			delay = 0;
        }

        delay = delay * 1000; // we are doing this every 1000 messages
        
        long nanos = (long)(delay * 1e9);

        LockSupport.parkNanos(nanos);
        
        // Flush small messages regularly
        if (this.getMessageSize() < 64 && count != 0 && count % 100_000 == 0) {
            try {nc.flush(Duration.ofSeconds(5));}catch(Exception e){}
        }
	}
}