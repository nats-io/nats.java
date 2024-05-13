// Copyright 2020 The NATS Authors
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

package io.nats.client;

import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.ListenerForTesting;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import static io.nats.client.utils.TestBase.standardConnection;
import static io.nats.client.utils.TestBase.subject;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequestTests {

    @Test
    public void testRequestNoResponder() throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true)) {
            Options optCancel = Options.builder().server(ts.getURI()).errorListener(new ListenerForTesting()).build();
            Options optReport = Options.builder().server(ts.getURI()).reportNoResponders().errorListener(new ListenerForTesting()).build();
            try (Connection ncCancel = standardConnection(optCancel);
                 Connection ncReport = standardConnection(optReport);
            )
            {
                assertThrows(CancellationException.class, () -> ncCancel.request(subject(999), null).get());
                ExecutionException ee = assertThrows(ExecutionException.class, () -> ncReport.request(subject(999), null).get());
                assertTrue(ee.getCause() instanceof JetStreamStatusException);
                assertTrue(ee.getMessage().contains("503 No Responders Available For Request"));

                ncCancel.jetStreamManagement().addStream(
                    StreamConfiguration.builder()
                        .name("testRequestNoResponder").subjects("trnrExists").storageType(StorageType.Memory)
                        .build());

                JetStream jsCancel = ncCancel.jetStream();
                JetStream jsReport = ncReport.jetStream();

                IOException ioe = assertThrows(IOException.class, () -> jsCancel.publish("not-exist", null));
                assertTrue(ioe.getMessage().contains("503"));
                ioe = assertThrows(IOException.class, () -> jsReport.publish("trnrNotExist", null));
                assertTrue(ioe.getMessage().contains("503"));
            }
        }
    }
}