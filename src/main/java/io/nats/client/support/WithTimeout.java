// Copyright 2021 The NATS Authors
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

package io.nats.client.support;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static io.nats.client.support.SneakyThrow.sneakyThrow;

public interface WithTimeout {
    /**
     * Executes a call with a specified timeout.
     * 
     * @param <T> is the result type of your function to call
     * @param <E> is the final type of exception which will be thrown.
     * @param call is a function to call
     * @param timeout is the max time that said function may execute before
     *    it is interrupted
     * @param createException is a function used to wrap the InterruptedException or
     *    TimeoutException so that you can chain your exceptions if desired.
     * @return the result of the function call.
     * @throws E if a timeout or interrupted exception occurs, after being filtered
     *    through createException.
     */
    static <T, E extends Throwable> T withTimeout(Callable<T> call, Duration timeout, Function<Throwable, E> createException) throws E {
        ExecutorService	executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit(call);
        try {
            return future.get(timeout.toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw createException.apply(ex);
        } catch (TimeoutException ex) {
            future.cancel(true);
            throw createException.apply(ex);
        } catch (ExecutionException ex) {
            throw sneakyThrow(ex.getCause());
        } finally {
            executor.shutdownNow();
        }
    }
}
