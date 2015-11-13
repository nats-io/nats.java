package io.nats.client;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;

public class Channel<T> extends LinkedBlockingQueue<T> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 8196885862801839881L;

		public Channel() {
			super();
		}

		public Channel(int capacity) {
			super(capacity);
			// TODO Auto-generated constructor stub
		}

		public Channel(Collection<T> c) {
			super(c);
			// TODO Auto-generated constructor stub
		}

	}