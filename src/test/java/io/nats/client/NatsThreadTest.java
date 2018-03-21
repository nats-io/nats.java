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

import static io.nats.client.UnitTestUtilities.sleep;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicBoolean;

@Category(UnitTest.class)
public class NatsThreadTest extends BaseUnitTest implements Runnable {
    private static final int NUM_THREADS = 5;

    @Test
    public void testRun() {
        final AtomicBoolean exThrown = new AtomicBoolean(false);
        Thread.UncaughtExceptionHandler exh = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread th, Throwable ex) {
                // System.out.println("Uncaught exception: " + ex);
                exThrown.set(true);
            }
        };
        NatsThread nt = new NatsThread(this);
        nt.setUncaughtExceptionHandler(exh);
        throwException = true;
        nt.start();
        try {
            Thread.sleep(100);
            nt.join();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        assertTrue(exThrown.get());
    }

    private boolean throwException = false;

    @Override
    public void run() {
        try {
            if (throwException) {
                throw new Error("just for a test");
            }
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            /* NOOP */
        }
    }

    @Test
    public void testNATSThreadRunnable() {
        NatsThread[] threads = new NatsThread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            NatsThread nt = new NatsThread(this);
            nt.start();
            threads[i] = nt;
        }
        sleep(100);

        assertTrue(NatsThread.getThreadsAlive() > 0);
        assertTrue(NatsThread.getThreadsCreated() > 0);
        try {
            for (int i = 0; i < NUM_THREADS; i++) {
                {
                    threads[i].join(500);
                }
            }
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    // @Test
    // public void testNATSThreadRunnableString() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetThreadsCreated() {
    // fail("Not yet implemented"); // TODO
    // }
    //
    // @Test
    // public void testGetThreadsAlive() {
    // fail("Not yet implemented"); // TODO
    // }
}
