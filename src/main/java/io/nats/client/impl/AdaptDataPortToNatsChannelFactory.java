package io.nats.client.impl;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import io.nats.client.Options;

/**
 * Adapter for legacy implementations of DataPort.
 * 
 * <p><b>NOTES:</b>
 * <ul>
 *   <li>The NatsConnection passed into connect() is a completely disconnected instance, and thus only the
 *       {@link NatsConnection#getOptions() getOptions()} method may be relied upon.
 *   <li>{@link DataSocker#upgradeToSecure() upgradeToSecure()} will never be called, but if there is a
 *       need to upgrade to a secure connection, this adapted DataPort instance will be wrapped automatically,
 *       and thus the TLS upgrade should happen transparently.
 *   <li>Only read/write calls that use <code>Future</code> are valid, and they are implemented by delaying the
 *       blocking read until the <code>Future.get()</code> method is called. This is to avoid consuming
 *       additional threads, but it also means that once the code base is moved to 100% async code, this
 *       adaptor will not work unless a thread pool is introduced here.
 * </ul>
 */
@Deprecated
public class AdaptDataPortToNatsChannelFactory implements NatsChannelFactory {
    private Supplier<DataPort> dataPortSupplier;

    public AdaptDataPortToNatsChannelFactory(Supplier<DataPort> dataPortSupplier) {
        this.dataPortSupplier = dataPortSupplier;
    }

    @Override
    public CompletableFuture<NatsChannel> connect(
        URI serverURI,
        Options options)
        throws IOException
    {
        Thread[] thread = new Thread[1];
        CompletableFuture<NatsChannel> future = new CompletableFuture<NatsChannel>() {
            @Override
            public boolean cancel(boolean ignored) {
                thread[0].interrupt();
                return true;
            }
        };
        thread[0] = new Thread(() -> {
            try {
                DataPort dataPort = dataPortSupplier.get();
                dataPort.connect(serverURI.toString(), new NatsConnection(options), Long.MAX_VALUE);
                future.complete(new Adapted(dataPort));
            } catch (Exception ex) {
                future.completeExceptionally(ex);
            }
        });
        thread[0].start();
        return future;
    }

    static class Adapted implements NatsChannel {
        private boolean isOpen = true;
        private DataPort dataPort;

        private Adapted(DataPort dataPort) {
            this.dataPort = dataPort;
        }

        @Override
        public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<Integer> read(ByteBuffer original) {
            ByteBuffer[] dst = new ByteBuffer[]{original};
            if (!original.hasArray()) {
                dst[0] = ByteBuffer.allocate(original.remaining());
            }
            return threadStealingFuture(() -> {
                int offset = dst[0].arrayOffset();
                int result = dataPort.read(dst[0].array(), offset + dst[0].position(), offset + dst[0].limit());
                if (original != dst[0]) {
                    dst[0].flip();
                    original.put(dst[0]);
                }
                return result;
            });
        }

        @Override
        public <A> void write(ByteBuffer src, A attachment, CompletionHandler<Integer, ? super A> handler) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<Integer> write(ByteBuffer original) {
            ByteBuffer[] src = new ByteBuffer[]{original};
            if (!original.hasArray() || 0 != original.arrayOffset() || 0 != original.position()) {
                src[0] = ByteBuffer.allocate(original.remaining());
                src[0].put(original.slice());
            }
            return threadStealingFuture(() -> {
                dataPort.write(src[0].array(), src[0].limit());
                if (original != src[0]) {
                    original.position(original.position() + src[0].position());
                }
                return src[0].position();
            });
        }

        @Override
        public void close() throws IOException {
            isOpen = false;
            dataPort.close();
        }

        @Override
        public boolean isOpen() {
            return isOpen;
        }

        @Override
        public boolean isSecure() {
            return false;
        }

        @Override
        public void shutdownInput() throws IOException {
            dataPort.shutdownInput();
        }
    }

    private interface IOCallable<T> extends Callable<T> {
        @Override
        T call() throws IOException;
    }

    private static <T> Future<T> threadStealingFuture(IOCallable<T> callable) {
        return new Future<T>() {
            boolean cancelled = false;
            boolean done = false;
            Thread running = null;
            T result;
            Exception error;

            @Override
            public synchronized boolean cancel(boolean mayInterruptIfRunning) {
                if (done) {
                    return true;
                }
                cancelled = true;
                if (null == running) {
                    return true;
                }
                if (!mayInterruptIfRunning) {
                    return false;
                }
                running.interrupt();
                return true;
            }

            @Override
            public boolean isCancelled() {
                return cancelled;
            }

            @Override
            public boolean isDone() {
                return done;
            }

            @Override
            public T get() throws InterruptedException, ExecutionException {
                synchronized (this) {
                    while (null != running) {
                        wait();
                    }
                    if (done) {
                        if (null != error) {
                            throw new ExecutionException(error);
                        }
                        return result;
                    }
                    if (cancelled) {
                        throw new CancellationException();
                    }
                    running = Thread.currentThread();
                }
                // Critically important to NOT hold any locks when calling callable.
                T finalResult = null;
                Exception finalError = null;
                try {
                    finalResult = callable.call();
                } catch (Exception ex) {
                    finalError = ex;
                }
                synchronized (this) {
                    running = null;
                    done = true;
                    error = finalError;
                    result = finalResult;
                    notifyAll();
                    if (null != error) {
                        throw new ExecutionException(error);
                    }
                    return result;
                }
            }

            @Override
            public T get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {

                AtomicBoolean throwTimeout = new AtomicBoolean(false);
                Thread thread = new Thread(() -> {
                    try {
                        Thread.sleep(unit.toMillis(timeout));
                    } catch (InterruptedException ex) {
                        return;
                    }
                    synchronized (this) {
                        throwTimeout.set(true);
                        if (null != running) {
                            running.interrupt();
                        }
                    }
                });
                thread.start();
                try {
                    return get();
                } catch (RuntimeException|ExecutionException|InterruptedException ex) {
                    if (throwTimeout.get()) {
                        throw new TimeoutException();
                    }
                    throw ex;
                } finally {
                    thread.interrupt();
                }
            }

        };
    }
}