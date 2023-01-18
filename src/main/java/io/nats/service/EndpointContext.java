package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Subscription;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonValue;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class EndpointContext {
    protected final Connection conn;
    protected final ServiceEndpoint se;
    protected final ServiceMessageHandler handler;
    protected final boolean recordStats;
    protected final String qGroup;

    protected final boolean internalDispatcher;
    protected final Dispatcher dispatcher;

    protected Subscription sub;

    protected ZonedDateTime started;
    protected String lastError;
    protected final AtomicLong numRequests;
    protected final AtomicLong numErrors;
    protected final AtomicLong processingTime;

    public EndpointContext(Connection conn, Dispatcher internalDispatcher, boolean recordStats, ServiceEndpoint se) {
        this.conn = conn;
        this.se = se;
        handler = se.getHandler();
        this.recordStats = recordStats;
        qGroup = recordStats ? ServiceUtil.QGROUP : null;

        if (se.getDispatcher() == null) {
            dispatcher = internalDispatcher;
            this.internalDispatcher = true;
        }
        else {
            dispatcher = se.getDispatcher();
            this.internalDispatcher = false;
        }

        numRequests = new AtomicLong();
        numErrors = new AtomicLong();
        processingTime = new AtomicLong();
        started = DateTimeUtils.gmtNow();
    }

    public void start() {
        sub = qGroup == null
            ? dispatcher.subscribe(se.getSubject(), this::onMessage)
            : dispatcher.subscribe(se.getSubject(), qGroup, this::onMessage);
        started = DateTimeUtils.gmtNow();
    }

    public void onMessage(Message msg) throws InterruptedException {
        long start = System.nanoTime();
        ServiceMessage smsg = new ServiceMessage(msg);
        try {
            if (recordStats) {
                incrementNumRequests();
            }
            handler.onMessage(smsg);
        }
        catch (Throwable t) {
            if (recordStats) {
                setError(t);
            }
            try {
                smsg.replyStandardError(conn, t.getMessage(), 500);
            } catch (RuntimeException ignore) {}
        }
        finally {
            if (recordStats) {
                addProcessingTime(System.nanoTime() - start);
            }
        }
    }

    public EndpointContext setSub(Subscription sub) {
        this.sub = sub;
        return this;
    }

    public EndpointStats getEndpointStats() {
        return new EndpointStats(
            se.getEndpoint().getName(),
            se.getSubject(),
            numRequests.get(),
            numErrors.get(),
            processingTime.get(),
            lastError,
            getData(),
            started);
    }

    private JsonValue getData() {
        return se.getStatsDataSupplier() == null ? null : se.getStatsDataSupplier().get();
    }

    public void reset() {
        numRequests.set(0);
        numErrors.set(0);
        processingTime.set(0);
        lastError = null;
        started = DateTimeUtils.gmtNow();
    }

    void incrementNumRequests() {
        numRequests.incrementAndGet();
    }

    void setError(Throwable t) {
        numErrors.incrementAndGet();
        this.lastError = t.toString();
    }

    void addProcessingTime(long elapsed) {
        processingTime.addAndGet(elapsed);
    }

    public String getSubject() {
        return se.getSubject();
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public boolean isNotInternalDispatcher() {
        return !internalDispatcher;
    }

    public Subscription getSub() {
        return sub;
    }
}
