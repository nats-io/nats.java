package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.service.ServiceMessage;
import io.nats.service.ServiceUtil;
import io.nats.service.Stats;

public abstract class Context {
    protected final Connection conn;
    protected final Stats stats;
    private final String subject;
    private final Dispatcher dispatcher;
    private final boolean isInternalDispatcher;
    private final boolean recordStats;
    private final String qGroup;
    private Subscription sub;

    public Context(Connection conn, String subject, Dispatcher dispatcher, boolean isInternalDispatcher, Stats stats, boolean isServiceContext) {
        this.conn = conn;
        this.subject = subject;
        this.dispatcher = dispatcher;
        this.isInternalDispatcher = isInternalDispatcher;
        this.stats = stats;
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
        long requestNo = recordStats ? stats.incrementNumRequests() : -1;
        long start = 0;
        try {
            start = System.nanoTime();
            subOnMessage(msg);
        } catch (Throwable t) {
            if (recordStats) {
                stats.incrementNumErrors();
                stats.setLastError(t.toString());
            }
            try {
                ServiceMessage.replyStandardError(conn, msg, t.getMessage(), 500);
            } catch (Exception ignore) {}
        } finally {
            if (recordStats) {
                long total = stats.addTotalProcessingTime(System.nanoTime() - start);
                stats.setAverageProcessingTime(total / requestNo);
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

    public Stats getStats() {
        return stats;
    }

    public Subscription getSub() {
        return sub;
    }
}
