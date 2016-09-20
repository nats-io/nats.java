/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

/**
 * This is the Java client API for NATS, including classes and interfaces for interacting with the
 * NATS server.
 * 
 * <H3>NATS Applications</H3>
 * 
 * <p>A NATS application is composed of the following parts:
 * 
 * <ul>
 * 
 * <li>NATS server (<code>gnatsd</code>) - a broker-style server that terminates and manages all
 * NATS client connections and routes messages between clients based on interest in the form of
 * {@link Subscription} objects</li>
 * 
 * <li>NATS clients - the programs, implemented in various languages, that connect to the NATS
 * server and send and receive NATS messages</li>
 * 
 * <li>NATS messages - {@link Message} object are used to communicate data/information between NATS
 * clients using the NATS protocol</li>
 * 
 * </ul>
 * 
 * @author Larry McQueary
 *
 */
package io.nats.client;
