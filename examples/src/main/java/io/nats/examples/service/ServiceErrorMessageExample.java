// Copyright 2026 The NATS Authors
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

package io.nats.examples.service;

import io.nats.client.*;
import io.nats.service.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * This example demonstrates a service that responds with errors,
 * and a requester that uses a ServiceReplyMessage to tell an error reply from a normal reply.
 */
public class ServiceErrorMessageExample {
    public static void main(String[] args) throws IOException {

        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .errorListener(new ErrorListener() {})
            .build();

        try (Connection nc = Nats.connect(options)) {
            // The endpoint divides two numbers given as "dividend/divisor".
            // It responds with an error when the request cannot be handled.
            ServiceEndpoint seDivide = ServiceEndpoint.builder()
                .endpointName("DivideEndpoint")
                .endpointSubject("divide")
                .handler(msg -> handleDivide(nc, msg))
                .build();

            Service service = new ServiceBuilder()
                .connection(nc)
                .name("DivideService")
                .version("0.0.1")
                .addServiceEndpoint(seDivide)
                .build();

            service.startService();

            // ----------------------------------------------------------------------------------------------------
            // Call the service. Some requests succeed and some get an error reply.
            // ----------------------------------------------------------------------------------------------------
            String[] requests = {"10/4", "10/0", "ten/4", "10", "10/four"};
            for (String request : requests) {
                Message reply = nc.request("divide", request.getBytes(StandardCharsets.UTF_8)).get(1, TimeUnit.SECONDS);
                ServiceReplyMessage srm = new ServiceReplyMessage(reply);
                if (srm.isError()) {
                    System.out.printf("Request: %-9s isError: %-5s Code: %-4s Description: %s%n",
                        request, srm.isError(), srm.getErrorCode(), srm.getErrorDescription());
                }
                else {
                    System.out.printf("Request: %-9s isError: %-5s Result: %s%n",
                        request, srm.isError(), new String(srm.getData(), StandardCharsets.UTF_8));
                }
            }

            // ----------------------------------------------------------------------------------------------------
            // The endpoint stats count the errors and keep the last one
            // ----------------------------------------------------------------------------------------------------
            EndpointStats es = service.getEndpointStats("DivideEndpoint");
            System.out.println("\nRequests: " + es.getNumRequests() + ", errors: " + es.getNumErrors()
                + ", last error: " + es.getLastError());

            service.stop();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void handleDivide(Connection nc, ServiceMessage smsg) {
        String request = new String(smsg.getData(), StandardCharsets.UTF_8);
        String[] parts = request.split("/");
        if (parts.length != 2) {
            // a code and a description
            smsg.respondError(nc, 400, "Request must be in the form dividend/divisor");
            return;
        }

        long dividend;
        try {
            dividend = Long.parseLong(parts[0]);
        }
        catch (NumberFormatException e) {
            // a description only, no code header is sent
            smsg.respondError(nc, "Dividend is not a number");
            return;
        }

        long divisor;
        try {
            divisor = Long.parseLong(parts[1]);
        }
        catch (NumberFormatException e) {
            // a code only, no description header is sent
            smsg.respondError(nc, 422);
            return;
        }

        if (divisor == 0) {
            smsg.respondError(nc, 400, "Cannot divide by zero");
            return;
        }

        smsg.respond(nc, dividend + " / " + divisor + " = " + (dividend / divisor));
    }
}
