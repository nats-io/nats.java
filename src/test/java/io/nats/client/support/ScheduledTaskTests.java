package io.nats.client.support;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.utils.TestBase.variant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
        String id = "100-" + variant();
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

    @Test
    public void testThrowingTickIsReportedAndScheduleSurvives() throws InterruptedException {
        ScheduledThreadPoolExecutor stpe = testExecutor();
        try {
            AtomicInteger ticks = new AtomicInteger();
            List<Throwable> reported = new CopyOnWriteArrayList<>();
            ScheduledTask task = new ScheduledTask(stpe, 50, TimeUnit.MILLISECONDS,
                () -> { throw new RuntimeException("tick " + ticks.incrementAndGet()); },
                reported::add);

            Thread.sleep(500);
            assertFalse(task.isDone());
            task.shutdown();
            assertTrue(task.isDone());
            Thread.sleep(100); // let any in-flight tick finish reporting

            assertTrue(ticks.get() >= 3, "ticks must keep firing after a tick throws, saw " + ticks.get());
            assertEquals(ticks.get(), reported.size());
            assertInstanceOf(RuntimeException.class, reported.get(0));
        }
        finally {
            stpe.shutdownNow();
        }
    }

    @Test
    public void testThrowingTickWithoutReporterScheduleStillSurvives() throws InterruptedException {
        ScheduledThreadPoolExecutor stpe = testExecutor();
        try {
            AtomicInteger ticks = new AtomicInteger();
            ScheduledTask task = new ScheduledTask(stpe, 50, TimeUnit.MILLISECONDS,
                () -> { throw new RuntimeException("tick " + ticks.incrementAndGet()); });

            Thread.sleep(500);
            assertTrue(ticks.get() >= 3, "ticks must keep firing without a reporter, saw " + ticks.get());
            assertFalse(task.isDone());
            task.shutdown();
        }
        finally {
            stpe.shutdownNow();
        }
    }

    @Test
    public void testThrowingReporterDoesNotKillSchedule() throws InterruptedException {
        ScheduledThreadPoolExecutor stpe = testExecutor();
        try {
            AtomicInteger ticks = new AtomicInteger();
            ScheduledTask task = new ScheduledTask(stpe, 50, TimeUnit.MILLISECONDS,
                () -> { throw new RuntimeException("tick " + ticks.incrementAndGet()); },
                t -> { throw new IllegalStateException("reporter is broken too"); });

            Thread.sleep(500);
            assertTrue(ticks.get() >= 3, "ticks must keep firing when the reporter throws, saw " + ticks.get());
            assertFalse(task.isDone());
            task.shutdown();
        }
        finally {
            stpe.shutdownNow();
        }
    }

    @Test
    public void testFatalErrorIsReportedAndScheduleDies() throws InterruptedException {
        ScheduledThreadPoolExecutor stpe = testExecutor();
        try {
            AtomicInteger ticks = new AtomicInteger();
            List<Throwable> reported = new CopyOnWriteArrayList<>();
            ScheduledTask task = new ScheduledTask(stpe, 50, TimeUnit.MILLISECONDS,
                () -> { ticks.incrementAndGet(); throw new OutOfMemoryError("simulated"); },
                reported::add);

            Thread.sleep(500);
            assertEquals(1, ticks.get(), "a fatal error must end the schedule after the first tick");
            assertEquals(1, reported.size());
            assertInstanceOf(OutOfMemoryError.class, reported.get(0));
            assertTrue(task.isDone());
        }
        finally {
            stpe.shutdownNow();
        }
    }

    private static ScheduledThreadPoolExecutor testExecutor() {
        ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(1);
        stpe.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        stpe.setRemoveOnCancelPolicy(true);
        return stpe;
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
