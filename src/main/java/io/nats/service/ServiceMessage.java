// Copyright 2023-2026 The NATS Authors
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
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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
    private byte[] responseData;
    private Headers responseHeaders;
    private String responseErrorDescription;
    private Integer responseErrorCode;

    ServiceMessage(@NonNull Message message) {
        this.message = message;
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a byte array
     */
    public void respond(@NonNull Connection conn, byte @Nullable [] response) {
        _respond(conn, response, null, null, null);
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a string
     */
    public void respond(@NonNull Connection conn, @Nullable String response) {
        _respond(conn, toBytes(response), null, null, null);
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a {@link JsonSerializable} object
     */
    public void respond(@NonNull Connection conn, @Nullable JsonSerializable response) {
        _respond(conn, toBytes(response), null, null, null);
    }

    /**
     * Respond to a service request message with a response and custom headers.
     * @param conn the NATS connection
     * @param response the response payload in the form of a byte array
     * @param headers the custom headers
     */
    public void respond(@NonNull Connection conn, byte @Nullable [] response, @Nullable Headers headers) {
        _respond(conn, response, headers, null, null);
    }

    /**
     * Respond to a service request message with a response and custom headers.
     * @param conn the NATS connection
     * @param response the response payload in the form of a string
     * @param headers the custom headers
     */
    public void respond(@NonNull Connection conn, @Nullable String response, @Nullable Headers headers) {
        _respond(conn, toBytes(response), headers, null, null);
    }

    /**
     * Respond to a service request message.
     * @param conn the NATS connection
     * @param response the response payload in the form of a {@link JsonSerializable} object
     * @param headers the custom headers
     */
    public void respond(@NonNull Connection conn, @Nullable JsonSerializable response, @Nullable Headers headers) {
        _respond(conn, toBytes(response), headers, null, null);
    }

    /**
     * Respond to a service request message with a standard error.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the error text becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param errorText the error message text
     * @param errorCode the error message code
     * @deprecated use {@link #respondError(Connection, int, String)}
     */
    @Deprecated
    public void respondStandardError(@NonNull Connection conn, @Nullable String errorText, int errorCode) {
        _respond(conn, null, null, errorText, errorCode);
    }

    /**
     * Respond to a service request message with an error.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description) {
        _respond(conn, null, null, description, code);
    }

    /**
     * Respond to a service request message with an error and custom headers.
     * The error headers are added to a copy of the custom headers.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     * @param headers the custom headers
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description, @Nullable Headers headers) {
        _respond(conn, null, headers, description, code);
    }

    /**
     * Respond to a service request message with an error and a response.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     * @param response the response payload in the form of a byte array
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description, byte @Nullable [] response) {
        _respond(conn, response, null, description, code);
    }

    /**
     * Respond to a service request message with an error and a response.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     * @param response the response payload in the form of a string
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description, @Nullable String response) {
        _respond(conn, toBytes(response), null, description, code);
    }

    /**
     * Respond to a service request message with an error and a response.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     * @param response the response payload in the form of a {@link JsonSerializable} object
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description, @Nullable JsonSerializable response) {
        _respond(conn, toBytes(response), null, description, code);
    }

    /**
     * Respond to a service request message with an error, a response and custom headers.
     * The error headers are added to a copy of the custom headers.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     * @param response the response payload in the form of a byte array
     * @param headers the custom headers
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description, byte @Nullable [] response, @Nullable Headers headers) {
        _respond(conn, response, headers, description, code);
    }

    /**
     * Respond to a service request message with an error, a response and custom headers.
     * The error headers are added to a copy of the custom headers.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     * @param response the response payload in the form of a string
     * @param headers the custom headers
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description, @Nullable String response, @Nullable Headers headers) {
        _respond(conn, toBytes(response), headers, description, code);
    }

    /**
     * Respond to a service request message with an error, a response and custom headers.
     * The error headers are added to a copy of the custom headers.
     * If this is sent before the handler returns, it is counted in the endpoint's
     * num_errors and the description becomes the endpoint's last_error.
     * @param conn the NATS connection
     * @param code the error code, or null to send no {@value #NATS_SERVICE_ERROR_CODE} header
     * @param description the error description
     * @param response the response payload in the form of a {@link JsonSerializable} object
     * @param headers the custom headers
     */
    public void respondError(@NonNull Connection conn, @Nullable Integer code, @NonNull String description, @Nullable JsonSerializable response, @Nullable Headers headers) {
        _respond(conn, toBytes(response), headers, description, code);
    }

    private static byte @Nullable [] toBytes(@Nullable String response) {
        return response == null ? null : response.getBytes(StandardCharsets.UTF_8);
    }

    private static byte @Nullable [] toBytes(@Nullable JsonSerializable response) {
        return response == null ? null : response.serialize();
    }

    // a response with an error description or code is an error; an error header is sent only for a value given
    private void _respond(@NonNull Connection conn, byte @Nullable [] data, @Nullable Headers headers,
                          @Nullable String errorDescription, @Nullable Integer errorCode) {
        Headers h = headers;
        if (errorDescription != null || errorCode != null) {
            h = new Headers(headers);
            if (errorDescription != null) {
                h.put(NATS_SERVICE_ERROR, errorDescription);
            }
            if (errorCode != null) {
                h.put(NATS_SERVICE_ERROR_CODE, errorCode.toString());
            }
        }
        conn.publish(NatsMessage.builder().subject(message.getReplyTo()).data(data).headers(h).build());
        responseData = data;
        responseHeaders = h;
        responseErrorDescription = errorDescription;
        responseErrorCode = errorCode;
    }

    /**
     * The text of the last standard error sent in response to this message
     * @return the error text or null if no standard error was sent
     * @deprecated use {@link #getResponseErrorDescription()} and {@link #getResponseErrorCode()}
     */
    @Deprecated
    @Nullable
    String getStandardError() {
        if (responseErrorDescription != null) {
            return responseErrorDescription;
        }
        return responseErrorCode == null ? null : responseErrorCode.toString();
    }

    /**
     * The data of the last response sent to this message
     * @return the data or null if no response was sent or it had no data
     */
    byte @Nullable [] getResponseData() {
        return responseData;
    }

    /**
     * The headers of the last response sent to this message, including any error headers
     * @return the headers or null if no response was sent or it had no headers
     */
    @Nullable
    Headers getResponseHeaders() {
        return responseHeaders;
    }

    /**
     * The error description of the last response sent to this message
     * @return the error description or null if no response was sent or it had no error description
     */
    @Nullable
    String getResponseErrorDescription() {
        return responseErrorDescription;
    }

    /**
     * The error code of the last response sent to this message
     * @return the error code or null if no response was sent or it had no error code
     */
    @Nullable
    Integer getResponseErrorCode() {
        return responseErrorCode;
    }

    /**
     * the subject that this message was sent to
     * @return the subject that this message was sent to
     */
    @NonNull
    public String getSubject() {
        return message.getSubject();
    }

    /**
     * the subject the application is expected to send a reply message on
     * @return the subject the application is expected to send a reply message on
     */
    @Nullable
    public String getReplyTo() {
        return message.getReplyTo();
    }

    /**
     * true if there are headers
     * @return true if there are headers
     */
    public boolean hasHeaders() {
        return message.hasHeaders();
    }

    /**
     * the headers object for the message
     * @return the headers object for the message
     */
    @Nullable
    public Headers getHeaders() {
        return message.getHeaders();
    }

    /**
     * the data from the message
     * @return the data from the message
     */
    public byte @NonNull [] getData() {
        return message.getData();
    }
}
