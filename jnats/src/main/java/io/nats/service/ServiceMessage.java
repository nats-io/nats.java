// Copyright 2023 The NATS Authors
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

package io.nats.service;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.JsonSerializable;

import java.nio.charset.StandardCharsets;

/**
 * Service Message is service specific object that exposes the service relevant parts of a NATS Message.
 */
public class ServiceMessage {

    /**
     * Standard header name used to report the text of an error
     */
    public static final String NATS_SERVICE_ERROR = "Nats-Service-Error";

    /**
     * Standard header name used to report the code of an error
     */
    public static final String NATS_SERVICE_ERROR_CODE = "Nats-Service-Error-Code";

    private final Message message;

    ServiceMessage(Message message) {
        this.message = message;
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a byte array 
     */
    public void respond(Connection conn, byte[] response) {
        conn.publish(message.getReplyTo(), response);
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a string
     */
    public void respond(Connection conn, String response) {
        conn.publish(message.getReplyTo(), response.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a {@link JsonSerializable} object
     */
    public void respond(Connection conn, JsonSerializable response) {
        conn.publish(message.getReplyTo(), response.serialize());
    }

    /**
     * Respond to a service request message with a response and custom headers.
     * @param conn the NATS connection
     * @param response the response payload in the form of a byte array
     * @param headers the custom headers                 
     */
    public void respond(Connection conn, byte[] response, Headers headers) {
        conn.publish(NatsMessage.builder().subject(message.getReplyTo()).data(response).headers(headers).build());
    }

    /**
     * Respond to a service request message with a response and custom headers.
     * @param conn the NATS connection
     * @param response the response payload in the form of a string
     * @param headers the custom headers                 
     */
    public void respond(Connection conn, String response, Headers headers) {
        conn.publish(NatsMessage.builder().subject(message.getReplyTo()).data(response).headers(headers).build());
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a {@link JsonSerializable} object
     * @param headers the custom headers                 
     */
    public void respond(Connection conn, JsonSerializable response, Headers headers) {
        conn.publish(NatsMessage.builder().subject(message.getReplyTo()).data(response.serialize()).headers(headers).build());
    }

    /**
     * Respond to a service request message with a standard error.
     * @param conn the NATS connection
     * @param errorText the error message text
     * @param errorCode the error message code
     */
    public void respondStandardError(Connection conn, String errorText, int errorCode) {
        conn.publish(NatsMessage.builder()
            .subject(message.getReplyTo())
            .headers(new Headers()
                .put(NATS_SERVICE_ERROR, errorText)
                .put(NATS_SERVICE_ERROR_CODE, "" + errorCode))
            .build());
    }

    /**
     * @return the subject that this message was sent to
     */
    public String getSubject() {
        return message.getSubject();
    }

    /**
     * @return the subject the application is expected to send a reply message on
     */
    public String getReplyTo() {
        return message.getReplyTo();
    }

    /**
     * @return true if there are headers
     */
    public boolean hasHeaders() {
        return message.hasHeaders();
    }

    /**
     * @return the headers object for the message
     */
    public Headers getHeaders() {
        return message.getHeaders();
    }

    /**
     * @return the data from the message
     */
    public byte[] getData() {
        return message.getData();
    }
}
