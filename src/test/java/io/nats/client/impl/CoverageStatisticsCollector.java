// Copyright 2023 The NATS Authors
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

import io.nats.client.StatisticsCollector;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is simply to have a concrete implementation to test setting properties and calling the builder in Options
 */
public class CoverageStatisticsCollector implements StatisticsCollector {

    private final AtomicLong outMsgs = new AtomicLong();

    @Override
    public void setAdvancedTracking(boolean trackAdvanced) {

    }

    @Override
    public long getPings() {
        return 0;
    }

    @Override
    public long getReconnects() {
        return 0;
    }

    @Override
    public long getDroppedCount() {
        return 0;
    }

    @Override
    public long getOKs() {
        return 0;
    }

    @Override
    public long getErrs() {
        return 0;
    }

    @Override
    public long getExceptions() {
        return 0;
    }

    @Override
    public long getRequestsSent() {
        return 0;
    }

    @Override
    public long getInMsgs() {
        return 0;
    }

    @Override
    public long getOutMsgs() {
        return outMsgs.get();
    }

    @Override
    public long getInBytes() {
        return 0;
    }

    @Override
    public long getOutBytes() {
        return 0;
    }

    @Override
    public long getFlushCounter() {
        return 0;
    }

    @Override
    public long getOutstandingRequests() {
        return 0;
    }

    @Override
    public long getRepliesReceived() {
        return 0;
    }

    @Override
    public long getDuplicateRepliesReceived() {
        return 0;
    }

    @Override
    public long getOrphanRepliesReceived() {
        return 0;
    }

    @Override
    public void incrementPingCount() {

    }

    @Override
    public void incrementReconnects() {

    }

    @Override
    public void incrementDroppedCount() {

    }

    @Override
    public void incrementOkCount() {

    }

    @Override
    public void incrementErrCount() {

    }

    @Override
    public void incrementExceptionCount() {

    }

    @Override
    public void incrementRequestsSent() {

    }

    @Override
    public void incrementRepliesReceived() {

    }

    @Override
    public void incrementDuplicateRepliesReceived() {

    }

    @Override
    public void incrementOrphanRepliesReceived() {

    }

    @Override
    public void incrementInMsgs() {

    }

    @Override
    public void incrementOutMsgs() {
        outMsgs.incrementAndGet();
    }

    @Override
    public void incrementInBytes(long bytes) {

    }

    @Override
    public void incrementOutBytes(long bytes) {

    }

    @Override
    public void incrementFlushCounter() {

    }

    @Override
    public void incrementOutstandingRequests() {

    }

    @Override
    public void decrementOutstandingRequests() {

    }

    @Override
    public void registerRead(long bytes) {

    }

    @Override
    public void registerWrite(long bytes) {

    }
}
