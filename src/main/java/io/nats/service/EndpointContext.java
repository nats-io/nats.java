package io.nats.service;

import io.nats.client.*;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonValue;
import io.nats.service.api.EndpointStats;

import java.time.ZonedDateTime;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class EndpointContext {
    protected final Connection conn;
    protected final ServiceEndpoint se;
    protected final MessageHandler handler;
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
    protected final AtomicLong averageProcessingTime;

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
        averageProcessingTime = new AtomicLong();
        started = DateTimeUtils.gmtNow();
    }

    public void start() {
        sub = qGroup == null
            ? dispatcher.subscribe(se.getSubject(), this::onMessage)
            : dispatcher.subscribe(se.getSubject(), qGroup, this::onMessage);
        started = DateTimeUtils.gmtNow();
    }

    public void onMessage(Message msg) throws InterruptedException {
        long requestNo = recordStats ? incrementNumRequests() : -1;
        long start = 0;
        try {
            start = System.nanoTime();
            handler.onMessage(msg);
        }
        catch (Throwable t) {
            if (recordStats) {
                incrementNumErrors();
                setLastError(t.toString());
            }
            try {
                ServiceReplyUtils.replyStandardError(conn, msg, t.getMessage(), 500);
            } catch (Exception ignore) {}
        }
        finally {
            if (recordStats) {
                long total = addTotalProcessingTime(System.nanoTime() - start);
                setAverageProcessingTime(total / requestNo);
            }
        }
    }

    public EndpointContext setSub(Subscription sub) {
        this.sub = sub;
        return this;
    }

    public EndpointStats getEndpointStats() {
        JsonValue data = se.getStatsDataSupplier() == null
            ? null
            : se.getStatsDataSupplier().get();

        return new EndpointStats(
            se.getEndpoint().getName(),
            se.getSubject(),
            numRequests.get(),
            numErrors.get(),
            processingTime.get(),
            averageProcessingTime.get(),
            lastError,
            data,
            started);
    }

    public void reset() {
        numRequests.set(0);
        numErrors.set(0);
        processingTime.set(0);
        averageProcessingTime.set(0);
        lastError = null;
        started = DateTimeUtils.gmtNow();
    }

    // TODO can we combine some of these to reduce number of function calls?

    long incrementNumRequests() {
        return this.numRequests.incrementAndGet();
    }

    void incrementNumErrors() {
        this.numErrors.incrementAndGet();
    }

    long addTotalProcessingTime(long elapsed) {
        return this.processingTime.addAndGet(elapsed);
    }

    void setAverageProcessingTime(long averageProcessingTime) {
        this.averageProcessingTime.set(averageProcessingTime);
    }

    void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public String getSubject() {
        return se.getSubject();
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public boolean isInternalDispatcher() {
        return internalDispatcher;
    }

    public Subscription getSub() {
        return sub;
    }
}
