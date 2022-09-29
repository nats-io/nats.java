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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.ConsumerInfo;
import io.nats.client.api.SimpleConsumerConfiguration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static io.nats.client.SimpleConsumerOptions.DEFAULT_SCO_OPTIONS;
import static io.nats.client.support.Validator.validateNotNull;

public class NatsJetStreamSimplified extends NatsJetStreamImpl implements JetStreamSimplified {

    private final JetStream js;
    private final JetStreamManagement jsm;
    private final String stream;

    public NatsJetStreamSimplified(NatsConnection connection, JetStreamOptions jsOptions, String stream) throws IOException {
        super(connection, jsOptions);
        this.stream = stream;
        js = new NatsJetStream(connection, jsOptions);
        jsm = new NatsJetStreamManagement(connection, jsOptions);
    }

    @Override
    public JetStream getJetStream() {
        return js;
    }

    private static final long MAX_CACHE_AGE = 1000 * 60 * 5; // 5 minutes;

    static class CachedConsumerConfiguration {
        ConsumerConfiguration cc;
        long lastUsed;

        public CachedConsumerConfiguration(ConsumerConfiguration cc) {
            this.cc = cc;
            lastUsed = System.currentTimeMillis();
        }
    }

    private static final ConcurrentHashMap<String, CachedConsumerConfiguration> CONSUMER_CACHE = new ConcurrentHashMap<>();

    private ConsumerConfiguration _getConsumerConfiguration(String consumerName) throws JetStreamApiException, IOException {
        CachedConsumerConfiguration ccc = CONSUMER_CACHE.get(consumerName);
        if (ccc == null || (System.currentTimeMillis() - ccc.lastUsed > MAX_CACHE_AGE)) {
            ConsumerConfiguration cc = _getConsumerInfo(stream, consumerName).getConsumerConfiguration();
            CONSUMER_CACHE.put(consumerName, new CachedConsumerConfiguration(cc));
            return cc;
        }
        ccc.lastUsed = System.currentTimeMillis();
        return ccc.cc;
    }

    // ----------------------------------------------------------------------------------------------------
    // Read
    // ----------------------------------------------------------------------------------------------------

//    @Override
//    public JetStreamSubscription subscribe(String consumerName) throws IOException, JetStreamApiException {
//        validateNotNull(consumerName, "Consumer Name");
//        ConsumerConfiguration cc = _getConsumerConfiguration(consumerName);
//        PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
//            .stream(stream).configuration(cc).build();
//        return createSubscription(
//            cc.getFilterSubject(), null, null, null, false, true, true, pullOpts, null);
//    }

    @Override
    public SimpleIterateConsumer iterate(String consumerName, int limit) throws IOException, JetStreamApiException {
        return null;
    }

    @Override
    public SimpleConsumer listen(String consumerName, int limit, MessageHandler handler) throws IOException, JetStreamApiException {
        return null;
    }

    @Override
    public SimpleIterateConsumer endlessIterate(String consumerName) throws IOException, JetStreamApiException {
        validateNotNull(consumerName, "Consumer Name");
        ConsumerConfiguration cc = _getConsumerConfiguration(consumerName);
        return _read(DEFAULT_SCO_OPTIONS, cc, true);
    }

    @Override
    public SimpleIterateConsumer endlessIterate(String consumerName, SimpleConsumerOptions sco) throws IOException, JetStreamApiException {
        validateNotNull(consumerName, "Consumer Name");
        ConsumerConfiguration cc = _getConsumerConfiguration(consumerName);
        return _read(sco, cc, true);
    }

    @Override
    public SimpleIterateConsumer endlessIterate(SimpleConsumerConfiguration consumerConfiguration) throws IOException, JetStreamApiException {
        return _read(DEFAULT_SCO_OPTIONS, consumerConfiguration, false);
    }

    @Override
    public SimpleIterateConsumer endlessIterate(SimpleConsumerConfiguration consumerConfiguration, SimpleConsumerOptions sco) throws IOException, JetStreamApiException {
        return _read(sco, consumerConfiguration, false);
    }

    private NatsSimpleIterateConsumer _read(SimpleConsumerOptions sco, ConsumerConfiguration cc, boolean isSimplificationMode) throws IOException, JetStreamApiException {
        PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
            .stream(stream).configuration(cc).build();
        MessageManagerFactory messageManagerFactory =
            (lConn, lSo, lCc, lQueueMode, lSyncMode) ->
                new PullSimpleMessageManager(lConn, lSyncMode, sco);
        JetStreamSubscription sub = createSubscription(
            cc.getFilterSubject(), null, null, null, false, true, isSimplificationMode, pullOpts, messageManagerFactory);
        return new NatsSimpleIterateConsumer((NatsJetStreamPullSubscription)sub, sco);
    }

    // ----------------------------------------------------------------------------------------------------
    // Listen
    // ----------------------------------------------------------------------------------------------------
    @Override
    public SimpleConsumer endlessListen(String consumerName, MessageHandler handler) throws IOException, JetStreamApiException {
        return endlessListen(consumerName, handler, DEFAULT_SCO_OPTIONS);
    }

    @Override
    public SimpleConsumer endlessListen(String consumerName, MessageHandler handler, SimpleConsumerOptions sco) throws IOException, JetStreamApiException {
        validateNotNull(consumerName, "Consumer Name");
        ConsumerConfiguration cc = _getConsumerConfiguration(consumerName);
        return _listen(handler, sco, cc, true);
    }

    @Override
    public SimpleConsumer endlessListen(SimpleConsumerConfiguration consumerConfiguration, MessageHandler handler) throws IOException, JetStreamApiException {
        return _listen(handler, DEFAULT_SCO_OPTIONS, consumerConfiguration, false);
    }

    @Override
    public SimpleConsumer endlessListen(SimpleConsumerConfiguration consumerConfiguration, MessageHandler handler, SimpleConsumerOptions sco) throws IOException, JetStreamApiException {
        return _listen(handler, sco, consumerConfiguration, false);
    }

    private NatsSimpleConsumer _listen(MessageHandler handler, SimpleConsumerOptions sco, ConsumerConfiguration cc, boolean isSimplificationMode) throws IOException, JetStreamApiException {
        PullSubscribeOptions pullOpts = PullSubscribeOptions.builder()
            .stream(stream)
            .configuration(cc)
            .build();
        MessageManagerFactory messageManagerFactory =
            (lConn, lSo, lCc, lQueueMode, lSyncMode) ->
                new PullSimpleMessageManager(lConn, lSyncMode, sco);
        NatsDispatcher dispatcher = (NatsDispatcher)conn.createDispatcher();
        JetStreamSubscription sub = createSubscription(
            cc.getFilterSubject(), null, dispatcher, handler, false, true, isSimplificationMode, pullOpts, messageManagerFactory);
        return new NatsSimpleConsumer((NatsJetStreamSubscription)sub, sco);
    }

    // ----------------------------------------------------------------------------------------------------
    // Management
    // ----------------------------------------------------------------------------------------------------

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo addOrUpdateConsumer(SimpleConsumerConfiguration config) throws IOException, JetStreamApiException {
        return jsm.addOrUpdateConsumer(stream, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteConsumer(String consumer) throws IOException, JetStreamApiException {
        return jsm.deleteConsumer(stream, consumer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerInfo getConsumerInfo(String consumer) throws IOException, JetStreamApiException {
        return jsm.getConsumerInfo(stream, consumer);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<String> getConsumerNames() throws IOException, JetStreamApiException {
        return jsm.getConsumerNames(stream);
    }

    /**
     * {@inheritDoc}
     */
    public List<ConsumerInfo> getConsumers() throws IOException, JetStreamApiException {
        return jsm.getConsumers(stream);
    }
}
