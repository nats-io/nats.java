/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
package io.nats.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadFactory;

/**
 * NATSThreadFactory
 * <p/>
 * Custom thread factory
 *
 * @author Brian Goetz and Tim Peierls
 */
class NATSThreadFactory implements ThreadFactory {
	private final String poolName;
	private CountDownLatch startSignal;
	private CountDownLatch doneSignal;

	public NATSThreadFactory(String poolName)
	{
		this(poolName,null,null);
	}
	
	public NATSThreadFactory(String poolName, CountDownLatch startSignal,
			CountDownLatch doneSignal)
	{
		this.poolName = poolName;
		this.startSignal=startSignal;
		this.doneSignal=doneSignal;
	}
	
	public Thread newThread(Runnable runnable, CountDownLatch startSignal,
			CountDownLatch doneSignal) {
		return new NATSThread(runnable, poolName, startSignal, doneSignal);
	}

	@Override
	public Thread newThread(Runnable r) {
		return newThread(r, null, null);
	}

}