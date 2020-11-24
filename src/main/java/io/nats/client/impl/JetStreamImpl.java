package io.nats.client.impl;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import io.nats.client.ConsumerConfiguration;
import io.nats.client.ConsumerInfo;
import io.nats.client.Dispatcher;
import io.nats.client.JetStream;
import io.nats.client.Message;
import io.nats.client.MessageHandler;
import io.nats.client.PublishAck;
import io.nats.client.PublishOptions;
import io.nats.client.StreamConfiguration;
import io.nats.client.SubscribeOptions;
import io.nats.client.Subscription;

public class JetStreamImpl implements JetStream {

    private NatsConnection conn = null;
    private ConsumerConfiguration defaultConsumerConfig = null;
    private String defaultStream = null;
    PublishOptions defaultPubOpts = new PublishOptions(Duration.ofSeconds(5));
    SubscribeOptions defaultSubOpts;

    private static final String jSApiConsumerCreateT = "$JS.API.CONSUMER.CREATE.%s";
    private static final String jSApiDurableCreateT  = "$JS.API.CONSUMER.DURABLE.CREATE.%s.%s";

    JetStreamImpl(NatsConnection connection, String streamName, ConsumerConfiguration consumerConfig) {
        defaultConsumerConfig = consumerConfig;
        conn = connection;

        if (this.defaultStream != null) {
            defaultPubOpts.setStream(streamName);
        }
    }

    JetStreamImpl(NatsConnection connection) {
        defaultConsumerConfig = ConsumerConfiguration.builder().build();
        conn = connection;
    }    

    @Override
    public void setDefaultPublishOptions(PublishOptions options) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDefaultSubscribeOptions(SubscribeOptions options) {
        // TODO Auto-generated method stub

    }
    

    private ConsumerInfo createOrUpdateConsumer(String deliverySubject, SubscribeOptions subOptions) throws TimeoutException, InterruptedException, IOException {
        /* SubscribeOptions will ensure strean and consumer are set */
        String stream = subOptions.getStream();
        
        ConsumerConfiguration cc = subOptions.getConsumerConfiguration();
        String durable = cc.getDurable();

        cc.setDeliverySubject(deliverySubject);
        String requestJSON = cc.toJSON(stream);

        String subj;
        if (durable == null) {
            subj = String.format(jSApiConsumerCreateT, stream);
        } else {
            subj = String.format(jSApiDurableCreateT, stream, durable);
        }
        
        Message resp = null;
        resp = conn.request(subj, requestJSON.getBytes(), conn.getOptions().getConnectionTimeout());

        if (resp == null) {
            throw new TimeoutException("Consumer request to jetstream timed out.");
        }

        JetstreamAPIResponse jsResp = new JetstreamAPIResponse(resp.getData());
        if (jsResp.hasError()) {
            throw new IOException(jsResp.getError());
        }

        return new ConsumerInfo(jsResp.getResponse());
    }      

    @Override
    public void loadOrCreate(String stream, StreamConfiguration config) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadOrCreate(String name, ConsumerConfiguration config) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    @Override
    public PublishAck publish(String subject, byte[] payload) throws IOException, InterruptedException, TimeoutException {
        return this.publish(subject, payload, defaultPubOpts);
    }

    private static boolean isStreamSpecified(String streamName) {
        return streamName != null && !PublishOptions.unspecifiedStream.equals(streamName);
    }

    @Override
    public PublishAck publish(String subject, byte[] payload, PublishOptions options) throws IOException, InterruptedException, TimeoutException{
        if (options == null) {
            throw new IllegalArgumentException("Options are required");
        }
        Message resp = conn.request(subject, payload, options.getStreamTimeout());
        if (resp == null) {
            throw new TimeoutException("timeout waiting for jetstream");
        }
        PublishAckImpl ack = new PublishAckImpl(resp.getData());
            
        String ackStream = ack.getStream();
        if (ackStream == null || ackStream.length() == 0 || ack.getSeqno() == 0) {
            throw new IOException("Invalid jetstream ack.");
        }

        String pubStream = options.getStream(); 
        if (isStreamSpecified(pubStream) && !pubStream.equals(ackStream)) {
            throw new IOException("Expected ack from stream " + pubStream + ", received from: " + ackStream);
        }

        return ack;
    }

    NatsSubscription createSubscription(String subject, String queueName, NatsDispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException{
        if (options != null && subject == null) {
            subject = conn.createInbox();
        }
        NatsSubscription sub;

        if (dispatcher != null) {
            sub = dispatcher.subscribeImpl(subject, queueName, handler);
        } else {
            sub = conn.createSubscription(subject, queueName, dispatcher);
        }

        if (options != null) {
            try {
                createOrUpdateConsumer(subject, options);
            } catch (Exception e) {
                sub.unsubscribe();
                throw e;
            }
        }   
        return sub;
    }

    private static void checkName(String s, boolean allowEmpty, String varName) {
        if (!allowEmpty && (s == null || s.isEmpty())) {
            throw new IllegalArgumentException(varName + " cannot be null or empty.");
        }

        if (s == null) {
            return;
        }

        for (int i = 0; i < s.length(); i++){
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                throw new IllegalArgumentException(varName + " cannot contain spaces.");
            }
        }
    }

    private static void checkNull(Object s, String name) {
        if (s == null) {
            throw new IllegalArgumentException(name + "cannot be null");
        }
    }

    static boolean isValidStreamName(String s) {
        if (s == null) {
            return false;
        }
        return !s.contains(".") && !s.contains("*") && !s.contains(">");
    }

    @Override
    public Subscription subscribe(String subject, SubscribeOptions options)
            throws InterruptedException, TimeoutException, IOException {
        checkName(subject, true, "subject");
        return createSubscription(subject, null, null, null, options); 
    }

    @Override
    public Subscription subscribe(String subject, String queue, SubscribeOptions options)
            throws InterruptedException, TimeoutException, IOException {
        checkName(subject, true, "subject");
        checkName(queue, false, "queue");
        checkNull(options, "options");
        return createSubscription(subject, queue, null, null, options); 
    }

    @Override
    public Subscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler) throws InterruptedException, TimeoutException, IOException {
        checkName(subject, true, "subject");
        checkNull(dispatcher, "dispatcher");
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, null);
    }

    @Override
    public Subscription subscribe(String subject, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException {
        checkName(subject, true, "subject");
        checkNull(dispatcher, "dispatcher");
        checkNull(handler, "handler");
        checkNull(options, "options");     
        return createSubscription(subject, null, (NatsDispatcher) dispatcher, handler, options);
    }

    @Override
    public Subscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler) throws InterruptedException, TimeoutException, IOException {
        checkName(subject, true, "subject");
        checkName(queue, false, "queue");
        checkNull(dispatcher, "dispatcher");
        checkNull(handler, "handler");
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, null);
    }

    @Override
    public Subscription subscribe(String subject, String queue, Dispatcher dispatcher, MessageHandler handler, SubscribeOptions options) throws InterruptedException, TimeoutException, IOException {
        checkName(subject, true, "subject");
        checkName(queue, false, "queue");
        checkNull(dispatcher, "dispatcher");
        checkNull(handler, "handler");
        checkNull(options, "options");      
        return createSubscription(subject, queue, (NatsDispatcher) dispatcher, handler, options);
    }
}
