package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.service.ServiceUtil;
import io.nats.service.Stats;
import io.nats.service.StatsDataSupplier;

import static io.nats.service.ServiceUtil.toDiscoverySubject;

public class StatsContext extends Context {

    private final StatsDataSupplier sds;

    public StatsContext(Connection conn, String serviceName, String serviceId,
                        Dispatcher dispatcher, boolean internalDispatcher,
                        Stats stats, StatsDataSupplier sds) {
        super(conn, toDiscoverySubject(ServiceUtil.STATS, serviceName, serviceId),
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
