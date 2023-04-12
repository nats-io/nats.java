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

package io.nats.examples;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class ReportNoResponders {
    public static void main(String[] args) throws IOException {

        // without new option
        Options options = new Options.Builder().server(Options.DEFAULT_URL).build();
        try (Connection nc = Nats.connect(options)) {
            CompletableFuture<Message> future = nc.request("no-one-is-listening", null);
            try {
                future.get();
            }
            catch (Exception e) {
                System.out.println(e);
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }

        // new option
        options = new Options.Builder().server(Options.DEFAULT_URL)
            .reportNoResponders()
            .build();

        try (Connection nc = Nats.connect(options)) {
            CompletableFuture<Message> future = nc.request("no-one-is-listening", null);
            try {
                future.get();
            }
            catch (Exception e) {
                System.out.println(e);
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}
