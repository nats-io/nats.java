package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.service.Stats;
import io.nats.service.StatsData;

import java.util.function.Supplier;

import static io.nats.service.ServiceUtil.STATS;
import static io.nats.service.ServiceUtil.toDiscoverySubject;

/**
 * SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
 */
public class StatsContext extends Context {

    private final Supplier<StatsData> sds;

    public StatsContext(Connection conn, String serviceName, String serviceId,
                        Dispatcher dispatcher, boolean internalDispatcher,
                        Stats stats, Supplier<StatsData> sds) {
        super(conn, toDiscoverySubject(STATS, serviceName, serviceId),
            dispatcher, internalDispatcher, stats, false);
        this.sds = sds;
    }

    @Override
    protected void subOnMessage(Message msg) throws InterruptedException {
        if (sds != null) {
            stats.setData(sds.get());
        }
        conn.publish(msg.getReplyTo(), stats.serialize());
    }
}
