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

import io.nats.client.NUID;

import java.text.NumberFormat;


public class NUIDBenchmarks {

    public static void main(String args[]) {
        benchmarkGlobalNUIDSpeed();
        System.out.println();
        benchmarkNUIDSpeed();
    }

    public static void benchmarkNUIDSpeed() {
        long count = 10_000_000;
        NUID nuid = new NUID();

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            nuid.next();
        }
        long elapsedNsec = System.nanoTime() - start;
        System.out.printf("Average generation time for %s NUIDs was %f ns\n",
                NumberFormat.getNumberInstance().format(count), (double) elapsedNsec / count);

    }

    public static void benchmarkGlobalNUIDSpeed() {
        long count = 10_000_000;

        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            NUID.nextGlobal();
        }
        long elapsedNsec = System.nanoTime() - start;
        System.out.printf("Average generation time for %s global NUIDs was %f ns\n",
                NumberFormat.getNumberInstance().format(count), (double) elapsedNsec / count);
    }
}