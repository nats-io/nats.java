package io.nats.client.support;

import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduledTaskTests {

    @Test
    public void testScheduledTask() throws InterruptedException {
        ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(3);
        stpe.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        stpe.setRemoveOnCancelPolicy(true);

        AtomicInteger counter400 = new AtomicInteger();
        SttRunnable sttr400 = new SttRunnable(100, counter400);
        ScheduledTask task400 = new ScheduledTask(stpe, 0, 400, TimeUnit.MILLISECONDS, sttr400);
        validateTaskPeriods(task400, 0, 400);

        AtomicInteger counter200 = new AtomicInteger();
        SttRunnable sttr200 = new SttRunnable(300, counter200);
        ScheduledTask task200 = new ScheduledTask(stpe, 0, 200, TimeUnit.MILLISECONDS, sttr200);
        validateTaskPeriods(task200, 0, 200);

        AtomicInteger counter100 = new AtomicInteger();
        SttRunnable sttr100 = new SttRunnable(400, counter100);
        String id = "100-" + TestBase.random();
        ScheduledTask task100 = new ScheduledTask(id, stpe, 0, 100, TimeUnit.MILLISECONDS, sttr100);
        validateTaskPeriods(task100, 0, 100);
        assertEquals(id, task100.getId());
        assertTrue(task100.toString().contains(id));
        assertTrue(task100.toString().contains("live"));
        assertTrue(task400.toString().contains("!done"));

        validateState(task400, sttr400, false, false, null);
        validateState(task200, sttr200, false, false, null);
        validateState(task100, sttr100, false, false, null);

        Thread.sleep(1600); // 3 x 500 = 1500, give a buffer to ensure three runs

        validateState(task400, sttr400, false, false, null);
        validateState(task200, sttr200, false, false, null);
        validateState(task100, sttr100, false, false, null);

        task400.shutdown();
        task200.shutdown();
        task100.shutdown();

        assertTrue(task400.isDone());
        assertTrue(task200.isDone());
        assertTrue(task200.isDone());

        Thread.sleep(500); // give one full cycle to make sure it's all done
        validateState(task400, sttr400, true, true, false);
        validateState(task200, sttr200, true, true, false);
        validateState(task100, sttr100, true, true, false);

        assertTrue(counter400.get() >= 3);
        assertTrue(counter200.get() >= 3);
        assertTrue(counter100.get() >= 3);

        // checks that the task is not re-run once shutdown
        int count = sttr400.counter.get();
        task400.run();
        assertEquals(count, sttr400.counter.get());
        assertTrue(task400.toString().contains("shutdown"));
        assertTrue(task400.toString().contains("/done"));
    }

    @SuppressWarnings("SameParameterValue")
    private static void validateTaskPeriods(ScheduledTask task, long expectedDelay, long expectedPeriod) {
        assertEquals(TimeUnit.MILLISECONDS.toNanos(expectedDelay), task.getInitialDelayNanos());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(expectedPeriod), task.getPeriodNanos());
    }

    static void validateState(ScheduledTask task, Runnable taskRunnable, boolean shutdown, boolean done, Boolean executing) {
        assertEquals(shutdown, task.isShutdown());
        assertEquals(done, task.isDone());
        if (executing != null) {
            assertEquals(executing, task.isExecuting());
        }
    }

    static class SttRunnable implements Runnable {
        final AtomicInteger counter;
        final long delay;

        public SttRunnable(long delay, AtomicInteger counter) {
            this.delay = delay;
            this.counter = counter;
        }

        @Override
        public void run() {
            counter.incrementAndGet();
            try {
                Thread.sleep(delay);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String toString() {
            return "SttRunnable{" +
                "counter=" + counter +
                ", delay=" + delay +
                '}';
        }
    }
}
