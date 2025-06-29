package io.nats.client.support;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScheduledTaskTests {

    @Test
    public void testScheduledTask() throws InterruptedException {
        ScheduledThreadPoolExecutor stpe = new ScheduledThreadPoolExecutor(3);
        stpe.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        stpe.setRemoveOnCancelPolicy(true);

        AtomicInteger counter3 = new AtomicInteger();
        ScheduledTask task3 = new ScheduledTask(stpe, 0, 300, TimeUnit.MILLISECONDS, counter3::incrementAndGet);

        AtomicInteger counter2 = new AtomicInteger();
        ScheduledTask task2 = new ScheduledTask(stpe, 0, 200, TimeUnit.MILLISECONDS, counter2::incrementAndGet);

        AtomicInteger counter1 = new AtomicInteger();
        ScheduledTask task1 = new ScheduledTask(stpe, 0, 100, TimeUnit.MILLISECONDS, counter1::incrementAndGet);

        Thread.sleep(1800);

        task3.shutdown();
        task2.shutdown();
        task1.shutdown();

        assertTrue(counter3.get() >= 5);
        assertTrue(counter2.get() >= 8);
        assertTrue(counter1.get() >= 17);
    }
}
