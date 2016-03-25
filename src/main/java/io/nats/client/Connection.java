/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A {@code Connection} object is a client's active connection to NATS.
 */
public interface Connection extends AbstractConnection {
    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject}.
     * 
     * @param subject the subject to publish the message to
     * @param data the message payload
     * @throws IOException if an I/O error is encountered
     */
    public void publish(String subject, byte[] data) throws IOException;

    /**
     * Publishes a message to a subject. The subject is set via {@link Message#setSubject(String)}
     * or the {@link Message#Message(String, String, byte[])} constructor.
     * 
     * @param msg the {@code Message} to publish
     * @throws IOException if an I/O error is encountered
     */
    public void publish(Message msg) throws IOException;

    /**
     * Publishes the payload specified by {@code data} to the subject specified by {@code subject},
     * with an optional reply subject. If {@code reply} is {@code null}, the behavior is identical
     * to {@link #publish(String, byte[])}
     * 
     * @param subject the subject to publish the message to
     * @param reply the subject to which subscribers should send responses
     * @param data the message payload
     * @throws IOException if an I/O error is encountered
     */
    public void publish(String subject, String reply, byte[] data) throws IOException;

    /**
     * Publishes a request message to the specified subject, waiting up to {@code timeout} msec for
     * a response.
     * 
     * @param subject the subject to publish the request message to
     * @param data the request message payload
     * @param timeout how long to wait for a response message (in msec)
     * @return the response message
     * @throws IOException if a connection-related error occurs
     * @throws TimeoutException if {@code timeout} elapses before a message is returned
     */
    public Message request(String subject, byte[] data, long timeout)
            throws TimeoutException, IOException;

    /**
     * Publishes a request message to the specified subject, waiting for a response until one is
     * available.
     * 
     * @param subject the subject to publish the request message to
     * @param data the message payload
     * @return the response message
     * @throws IOException if a connection-related error occurs
     * @throws TimeoutException if {@code timeout} elapses before a message is returned
     */
    public Message request(String subject, byte[] data) throws TimeoutException, IOException;

    Message request(String subject, byte[] data, long timeout, TimeUnit unit)
            throws TimeoutException, IOException;

}
