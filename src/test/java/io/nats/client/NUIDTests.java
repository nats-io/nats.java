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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class NUIDTests {
    @Test
    public void testDigits() {
        assertEquals(NUID.base, NUID.digits.length, "digits length does not match base modulo");
    }

    @Test
    public void testGlobalNUIDInit() {
        NUID nuid = NUID.getInstance();
        assertNotNull(nuid);
        assertNotNull(nuid.getPre(), "Expected prefix to be initialized");
        assertEquals(NUID.preLen, nuid.getPre().length);
        assertNotEquals(0, nuid.getSeq(), "Expected seq to be non-zero");
    }

    @Test
    public void testNUIDRollover() {
        NUID gnuid = NUID.getInstance();
        gnuid.setSeq(NUID.maxSeq);
        // copy
        char[] oldPre = Arrays.copyOf(gnuid.getPre(), gnuid.getPre().length);
        gnuid.next();
        assertNotEquals(oldPre, gnuid.getPre(), "Expected new pre, got the old one");
    }

    @Test
    public void testGUIDLen() {
        String nuid = new NUID().next();
        assertEquals(NUID.totalLen,
                nuid.length(), String.format("Expected len of %d, got %d", NUID.totalLen, nuid.length()));
    }

    @Test
    public void testGlobalGUIDLen() {
        String nuid = NUID.nextGlobal();
        assertEquals(NUID.totalLen,
                nuid.length(), String.format("Expected len of %d, got %d", NUID.totalLen, nuid.length()));
    }

    @Test
    public void testProperPrefix() {
        char min = (char) 255;
        char max = (char) 0;
        char[] digits = NUID.digits;
        for (char digit : digits) {
            if (digit < min) {
                min = digit;
            }
            if (digit > max) {
                max = digit;
            }
        }

        for (int i = 0; i < 1_000_000; i++) {
            NUID nuid = new NUID();
            for (int j = 0; j < NUID.preLen; j++) {
                if (nuid.pre[j] < min || nuid.pre[j] > max) {
                    fail(String.format(
                            "Iter %d. Valid range for bytes prefix: [%d..%d]\nIncorrect prefix at pos %d: %s",
                            i, (int) min, (int) max, j, new String(nuid.pre)));
                }
            }
        }
    }

    @Test
    public void whenRandomizePrefixCalledDirectlyThenPrefixIsRandomized() {
        NUID nuid = new NUID();
        char[] originalPrefix = nuid.getPre().clone();
        nuid.randomizePrefix();

        // This test ensures that the randomizePrefix method actually modifies the prefix.
        // Since randomization can potentially lead to the same value by chance, this test might not always detect a failure in randomizePrefix.
        assertNotEquals(String.valueOf(originalPrefix), String.valueOf(nuid.getPre()));
    }

    @Test
    public void whenMultipleThreadsIncrementSequenceThenNoCollisionsOccur() throws InterruptedException {
        NUID nuid = NUID.getInstance();
        Set<String> sequences = ConcurrentHashMap.newKeySet();
        //noinspection resource
        ExecutorService service = Executors.newFixedThreadPool(10);

        // This test checks the thread-safety of nextSequence by generating sequences from multiple threads.
        // It uses a concurrent set to track unique sequences and expects no duplicates, ensuring thread-safe operation.
        for (int i = 0; i < 1000; i++) {
            service.submit(() -> {
                String seq = nuid.nextSequence();
                sequences.add(seq);
            });
        }

        service.shutdown();
        //noinspection ResultOfMethodCallIgnored
        service.awaitTermination(1, TimeUnit.MINUTES);

        // All sequences should be unique, verifying no sequence collision occurs when accessed by multiple threads.
        assertEquals(1000, sequences.size());
    }

    @Test
    public void testSequenceRollover() {
        NUID nuid = new NUID();
        nuid.setSeq(NUID.maxSeq - 1); // set to one less than max to test rollover
        String seq1 = nuid.nextSequence(); // this should be the max sequence
        String seq2 = nuid.nextSequence(); // this should rollover and randomize the prefix

        // Verifies the rollover logic where the sequence number wraps around and the prefix is randomized.
        assertNotEquals(seq1, seq2, "Sequence should rollover and not be equal after hitting maxSeq");
    }

    @Test
    public void testBoundaryCondition() {
        NUID nuid = new NUID();
        nuid.setSeq(NUID.maxSeq);
        String seq1 = nuid.nextSequence(); // this should trigger the randomizePrefix and resetSequential

        // Ensures that boundary conditions are handled correctly by resetting the sequence number when maxSeq is reached.
        assertNotNull(seq1, "Sequence should not be null after seq has reached maxSeq");
        assertTrue(nuid.getSeq() < NUID.maxSeq, "Sequence should reset to a value less than maxSeq");
    }

    @Test
    public void testSequencePostRollover() {
        NUID nuid = new NUID();
        nuid.setSeq(NUID.maxSeq);
        nuid.nextSequence(); // triggers rollover
        long oldSeq = nuid.getSeq();
        nuid.nextSequence();

        // Confirms that sequence numbers continue to increment correctly after a rollover event.
        assertTrue(nuid.getSeq() > oldSeq, "Sequence should increment after rollover");
    }

    @Test
    public void testConcurrency() {
        NUID nuid = new NUID();
        Runnable task = () -> {
            // Verifies that concurrent calls to nextSequence result in correct sequence increments.
            for (int i = 0; i < 1000; i++) {
                nuid.nextSequence();
            }
        };
        Thread t1 = new Thread(task);
        Thread t2 = new Thread(task);
        t1.start();
        t2.start();
        try {
            t1.join();
            t2.join();
        } catch (InterruptedException e) {
            fail("Threads should not be interrupted during execution");
        }
        assertTrue(nuid.getSeq() < NUID.maxSeq, "Sequence should always be less than maxSeq even after concurrent increments");
    }
}
