/*******************************************************************************
 * Copyright (c) 2012, 2016 Apcera Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the MIT License (MIT)
 * which accompanies this distribution, and is available at
 * http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */
package io.nats.client;

import java.io.IOException;

public class SlowConsumerException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public SlowConsumerException() {
		this("nats: Slow Consumer, messages dropped");
	}

	public SlowConsumerException(String msg) {
		super(msg);
		// TODO Auto-generated constructor stub
	}

	public SlowConsumerException(Throwable e) {
		super(e);
		// TODO Auto-generated constructor stub
	}

	public SlowConsumerException(String string, Exception e) {
		super(string, e);
		// TODO Auto-generated constructor stub
	}

}
