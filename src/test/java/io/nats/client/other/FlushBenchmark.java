// Copyright 2015-2018 The NATS Authors
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

package io.nats.client.other;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.text.NumberFormat;

public class FlushBenchmark {
    public static void main(String args[]) throws InterruptedException {
        int flushes = 100_000;

        System.out.println("###");
        System.out.printf("### Running benchmark with %s flushes.\n",
                                NumberFormat.getInstance().format(flushes));
        System.out.println("###");

        try {
            Options options = new Options.Builder().turnOnAdvancedStats().build();
            Connection nc = Nats.connect(options);

            long start = System.nanoTime();
            for (int i=0; i<flushes; i++){
                nc.flush(null);
            }
            long end = System.nanoTime();

            nc.close();

            System.out.printf("### Total time to perform %s flushes was %s ms, %f ns/op\n",
                    NumberFormat.getInstance().format(flushes),
                    NumberFormat.getInstance().format((end - start) / 1_000_000L),
                    ((double) (end - start)) / ((double) (flushes)));
            System.out.printf("### This is equivalent to %s flushes/sec.\n",
                    NumberFormat.getInstance().format(1_000_000_000L * flushes / (end - start)));

            System.out.println("###");
            System.out.println("### Flush connection stats ####");
            System.out.println();
            System.out.print(nc.getStatistics().toString());
        } catch (Exception ex) {
            System.out.println("Exception running benchmark.");
            ex.printStackTrace();
        }
    }
}