package io.nats.client;

import org.slf4j.*;

class DefaultExceptionHandler implements ExceptionHandler {
	final static Logger logger = LoggerFactory.getLogger(ConnectionImpl.class);
	@Override
	public void onException(NATSException e) {
		logger.error("NATS async error:", e);
	}

}
