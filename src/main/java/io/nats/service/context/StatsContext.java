package io.nats.service.context;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.service.StatsData;
import io.nats.service.StatsResponse;

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
                        StatsResponse statsResponse, Supplier<StatsData> sds) {
        super(conn, toDiscoverySubject(STATS, serviceName, serviceId),
            dispatcher, internalDispatcher, statsResponse, false);
        this.sds = sds;
    }

    @Override
    protected void subOnMessage(Message msg) throws InterruptedException {
        if (sds != null) {
            statsResponse.setData(sds.get());
        }
        conn.publish(msg.getReplyTo(), statsResponse.serialize());
    }
}
