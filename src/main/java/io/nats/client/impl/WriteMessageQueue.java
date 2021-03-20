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

package io.nats.client.impl;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

class WriteMessageQueue extends MessageQueue {

    private NatsMessage currentPeek;

    public WriteMessageQueue(boolean singleReaderMode, int publishHighwaterMark, boolean discardWhenFull) {
        super(singleReaderMode, publishHighwaterMark, discardWhenFull);
    }

    public WriteMessageQueue(boolean singleReaderMode) {
        super(singleReaderMode);
    }

    public WriteMessageQueue(boolean singleReaderMode, int publishHighwaterMark) {
        super(singleReaderMode, publishHighwaterMark);
    }

    // Waits up to the timeout to try to accumulate multiple messages
    // Use the next field to read the entire set accumulated.
    // maxSize and maxMessages are both checked and if either is exceeded
    // the method returns.
    //
    // A timeout of 0 will wait forever (or until the queue is stopped/drained)
    //
    // Only works in single reader mode, because we want to maintain order.
    // accumulate reads off the concurrent queue one at a time, so if multiple
    // readers are present, you could get out of order message delivery.
    AccumulateResult accumulate(long maxSize, long maxMessages, Duration timeout)
            throws InterruptedException {

        if (!this.singleThreadedReader) {
            throw new IllegalStateException("Accumulate is only supported in single reader mode.");
        }

        if (!this.isRunning()) {
            return null;
        }

        NatsMessage msg = this.poll(timeout);

        if (msg == null) {
            return null;
        }

        AccumulateResult result = new AccumulateResult(msg);
        result.size = msg.estimateSizeInBytes();

        if (maxMessages <= 1 || result.size >= maxSize) {
            this.sizeInBytes.addAndGet(-result.size);
            this.length.decrementAndGet();
            return result;
        }

        result.count = 1;
        NatsMessage cursor = msg;

        while (cursor != null) {
            NatsMessage next = internalPeek();
            if (next != null && next != this.poisonPill) {
                long s = next.estimateSizeInBytes();

                if (maxSize<0 || (result.size + s) < maxSize) { // keep going
                    result.size += s;
                    result.count++;

                    cursor.next = getPeekAsPoll();
                    cursor = cursor.next;

                    if (result.count == maxMessages) {
                        break;
                    }
                } else { // One more is too far
                    break;
                }
            } else { // Didn't meet max condition
                break;
            }
        }

        this.sizeInBytes.addAndGet(-result.size);
        this.length.addAndGet(-result.count);

        return result;
    }

    @Override
    protected NatsMessage internalPeek() {
        if (currentPeek == null) {
            currentPeek = queue.poll();
        }
        return currentPeek;
    }

    @Override
    protected NatsMessage internalPoll() {
        return currentPeek == null ? queue.poll() : getPeekAsPoll();
    }

    @Override
    protected NatsMessage internalPoll(long timeout, TimeUnit unit) throws InterruptedException {
        return currentPeek == null ? queue.poll(timeout, unit) : getPeekAsPoll();
    }

    @Override
    protected NatsMessage getPeekAsPoll() {
        NatsMessage temp = currentPeek;
        currentPeek = null;
        return temp;
    }
}