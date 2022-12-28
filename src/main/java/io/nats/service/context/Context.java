package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.service.ServiceMessage;
import io.nats.service.ServiceUtil;
import io.nats.service.StatsResponse;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public abstract class Context {
    protected final Connection conn;
    protected final StatsResponse statsResponse;
    protected final String subject;
    protected final Dispatcher dispatcher;
    protected final boolean isInternalDispatcher;
    protected final boolean recordStats;
    protected final String qGroup;
    protected Subscription sub;

    public Context(Connection conn, String subject, Dispatcher dispatcher, boolean isInternalDispatcher, StatsResponse statsResponse, boolean isServiceContext) {
        this.conn = conn;
        this.subject = subject;
        this.dispatcher = dispatcher;
        this.isInternalDispatcher = isInternalDispatcher;
        this.statsResponse = statsResponse;
        recordStats = isServiceContext;
        qGroup = isServiceContext ? ServiceUtil.QGROUP : null;
    }

    public void start() {
        sub = qGroup == null
            ? dispatcher.subscribe(subject, this::onMessage)
            : dispatcher.subscribe(subject, qGroup, this::onMessage);
    }

    protected abstract void subOnMessage(Message msg) throws InterruptedException;

    public void onMessage(Message msg) throws InterruptedException {
        long requestNo = recordStats ? statsResponse.incrementNumRequests() : -1;
        long start = 0;
        try {
            start = System.nanoTime();
            subOnMessage(msg);
        } catch (Throwable t) {
            if (recordStats) {
                statsResponse.incrementNumErrors();
                statsResponse.setLastError(t.toString());
            }
            try {
                ServiceMessage.replyStandardError(conn, msg, t.getMessage(), 500);
            } catch (Exception ignore) {}
        } finally {
            if (recordStats) {
                long total = statsResponse.addTotalProcessingTime(System.nanoTime() - start);
                statsResponse.setAverageProcessingTime(total / requestNo);
            }
        }
    }

    public Context setSub(Subscription sub) {
        this.sub = sub;
        return this;
    }

    public String getSubject() {
        return subject;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public boolean isInternalDispatcher() {
        return isInternalDispatcher;
    }

    public StatsResponse getStats() {
        return statsResponse;
    }

    public Subscription getSub() {
        return sub;
    }
}
