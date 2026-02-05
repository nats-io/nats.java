package io.nats.examples.jetstream;
//package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.PriorityPolicy;
import io.nats.examples.jetstream.ResilientPublisher;


import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
* This example demonstrates an overflow consumer for priority groups.
*
* Priority groups are a new set of features allowing warm standby and
* dynamic worker pools processing messages on the same consumer.
*
*  This examples shows a standby worker activating when the backlog on a
*  consumer builds up "overflowing" messages into the standby worker.
*
*/
public class PriorityGroupsOverFlow {
	 private static final String STREAM = "priority-groups-overflow";
	 private static final String SUBJECT = "priority-groups-overflow";
	 private static final String CONSUMER_NAME = "overflow-consumer";
	 private static final String MESSAGE_PREFIX = "consume";
	 private static final String PRIORITY_GROUP = "group";
	 private static final int OVERFLOW_MIN_PENDING = 10;
	 private static final int PUBLISH_DELAY = 500;
	 private static final int PRE_FETCH_SIZE = 10;
	 private static final String SERVER = "nats://localhost:4222";


	 static Thread pubThread;

	 public static void primaryConsumer() {

		 Options options = Options.builder().servers(new String[] {SERVER, SERVER} ).build();
	     try (Connection nc = Nats.connect(options)) {

	         // get stream context, create consumer with overflow  priority policy and get the consumer context
	         StreamContext streamContext;
	         streamContext = nc.getStreamContext(STREAM);
	         ConsumerContext consumerContext;
	         consumerContext = streamContext.getConsumerContext(CONSUMER_NAME);

	         MessageHandler handler = msg -> {
	             msg.ack();
	             System.out.println("Consumed message in primary consumer: " + new String(msg.getData()));
	             //Consuming slower then we publish
	             //Eventually backlog will build that will trigger the overflow consumer to become active
	             Thread.sleep(PUBLISH_DELAY*2);
	         };

	         System.out.println("Starting primary consumer - should be active right away");

	         ConsumeOptions consumerOptions = new ConsumeOptions.Builder()
	        		 //We need to limit the number of messages we prefetch - otherwise the overflow will not get a chance to kick in
	        		 .batchSize(PRE_FETCH_SIZE)
	        		 .thresholdPercent(50)
	        		 .group(PRIORITY_GROUP)
	        		 .minPending(0)
	        		 .build();
	     	 // create the consumer and install handler
	     	 MessageConsumer consumer = consumerContext.consume(consumerOptions, handler);

	         //Just doing next()
	         /*
	     	 while ( consumerContext != null) {
	        	 Message msg = consumerContext.next();
	        	 msg.ack();

	         }
	         */
	     	//Wait for the publish thread to terminate
	     	 pubThread.join();

	     }
	     catch (Exception e) {
	    	 e.printStackTrace();
	         System.err.println("Exception should not handled, exiting.");
	         System.exit(-1);
	     }

	 }

	 public static void overflowConsumer() {
		 Options options = Options.builder().servers(new String[] {SERVER, SERVER} ).build();
	     try (Connection nc = Nats.connect(options)) {

	         // get stream context, create consumer with overflow  priority policy and get the consumer context
	         StreamContext streamContext;
	         streamContext = nc.getStreamContext(STREAM);
	         ConsumerContext consumerContext;
	         consumerContext = streamContext.getConsumerContext(CONSUMER_NAME);

	         MessageHandler handler = msg -> {
	             msg.ack();
	             System.out.println("WARNING: Consumed message in overflow consumer: " + new String(msg.getData()));
	             //Thread.sleep(PUBLISH_DELAY);
	         };

	         System.out.println("Starting overflow consumer - should become active after a few seconds when consumer backlog has built up.");

	         ConsumeOptions consumerOptions = new ConsumeOptions.Builder()
	        		 //.batchBytes(0)
	        		 .batchSize(PRE_FETCH_SIZE)
	        		 .group(PRIORITY_GROUP)
	        		 .minPending(OVERFLOW_MIN_PENDING)
	        		 .minAckPending(OVERFLOW_MIN_PENDING/2)
	        		 .build();
	     	 // create the consumer and install handler
	     	 MessageConsumer consumer = consumerContext.consume(consumerOptions, handler);

	     	//Wait for the publish thread to terminate
	     	 pubThread.join();

	     }
	     catch (Exception e) {
	    	 e.printStackTrace();
	         System.err.println("Exception should not handled, exiting.");
	         System.exit(-1);
	     }

	 }

	 public static void main(String[] args) {
	     Options options = Options.builder().servers(new String[] {SERVER, SERVER} ).build();
	     try (Connection nc = Nats.connect(options)) {

	         JetStreamManagement jsm = nc.jetStreamManagement();
	         createOrReplaceStream(jsm, STREAM, SUBJECT);

	         //Utility for filling the stream with some messages
	         System.out.println("Starting publish...");
	         //Publishing
	         ResilientPublisher publisher = new ResilientPublisher(nc, jsm, STREAM, SUBJECT).basicDataPrefix(MESSAGE_PREFIX).delay(PUBLISH_DELAY);

	         pubThread = new Thread(publisher);
	         pubThread.start();

	         // get stream context, create consumer with overflow  priority policy and get the consumer context
	         StreamContext streamContext;


	         streamContext = nc.getStreamContext(STREAM);
	         streamContext.createOrUpdateConsumer(ConsumerConfiguration.builder()
	        		 .durable(CONSUMER_NAME)
	        		 .priorityGroups(PRIORITY_GROUP)
	        		 .priorityPolicy(PriorityPolicy.Overflow)
	        		 .maxAckPending(20)
	        		 .build());

	         Thread primaryConsumer = new Thread(() -> {
	        	 primaryConsumer();
	         });
	         primaryConsumer.start();

	         Thread overflowConsumer = new Thread(() -> {
	        	 overflowConsumer();
	         });
	         overflowConsumer.start();

	         //Wait for the publish thread to terminate
	         pubThread.join();
	     }
	     catch (Exception e) {
	    	 e.printStackTrace();
	         System.err.println("Exception should not handled, exiting.");
	         System.exit(-1);
	     }
	 }
}
