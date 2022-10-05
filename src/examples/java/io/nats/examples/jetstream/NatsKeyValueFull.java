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

package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.StorageType;
import io.nats.examples.ExampleArgs;
import io.nats.examples.ExampleUtils;

/**
 * This example will demonstrate Key Value store.
 */
public class NatsKeyValueFull {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsKeyValueFull [-s server] [-buk bucket] [-d description]"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    private static final String BYTE_KEY = "byteKey";
    private static final String STRING_KEY = "stringKey";
    private static final String LONG_KEY = "longKey";
    private static final String NOT_FOUND = "notFound";

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleArgs.builder("Key Value Full Example", args, usageString)
            .defaultBucket("exampleBucket")
            .defaultDescription("Example Description")
            .build();

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            // get the kv management context
            KeyValueManagement kvm = nc.keyValueManagement();

            // create the bucket
            KeyValueConfiguration kvc = KeyValueConfiguration.builder()
                .name(exArgs.bucket)
                .description(exArgs.description)
                .maxHistoryPerKey(5)
                .storageType(StorageType.Memory)
                .build();

            KeyValueStatus kvs = kvm.create(kvc);
            System.out.println(kvs);

            // get the kv context for the specific bucket
            KeyValue kv = nc.keyValue(exArgs.bucket);

            // Put some keys. Each key is put in a subject in the bucket (stream)
            // The put returns the Revision number in the bucket (stream)
            System.out.println("\n1. Put");

            long seq = kv.put(BYTE_KEY, "Byte Value 1".getBytes());
            System.out.println("Revision number should be 1, got " + seq);

            seq = kv.put(STRING_KEY, "String Value 1");
            System.out.println("Revision number should be 2, got " + seq);

            seq = kv.put(LONG_KEY, 1);
            System.out.println("Revision number should be 3, got " + seq);

            // retrieve the values. all types are stored as bytes
            // so you can always get the bytes directly
            System.out.println("\n2. Get Value (Bytes)");

            byte[] bvalue = kv.get(BYTE_KEY).getValue();
            System.out.println(BYTE_KEY + " from getValue: " + new String(bvalue));

            bvalue = kv.get(STRING_KEY).getValue();
            System.out.println(STRING_KEY + " from getValue: " + new String(bvalue));

            bvalue = kv.get(LONG_KEY).getValue();
            System.out.println(LONG_KEY + " from getValue: " + new String(bvalue));

            // if you know the value is not binary and can safely be read
            // as a UTF-8 string, the getStringValue method is ok to use
            System.out.println("\n3. Get String Value");

            String svalue = kv.get(BYTE_KEY).getValueAsString();
            System.out.println(BYTE_KEY + " from getValueAsString: " + svalue);

            svalue = kv.get(STRING_KEY).getValueAsString();
            System.out.println(STRING_KEY + " from getValueAsString: " + svalue);

            svalue = kv.get(LONG_KEY).getValueAsString();
            System.out.println(LONG_KEY + " from getValueAsString: " + svalue);

            // if you know the value is a long, you can use
            // the getLongValue method
            // if it's not a number a NumberFormatException is thrown
            System.out.println("\n4. Get Long Value");

            Long lvalue = kv.get(LONG_KEY).getValueAsLong();
            System.out.println(LONG_KEY + " from getValueAsLong: " + lvalue);

            try {
                kv.get(STRING_KEY).getValueAsLong();
            }
            catch (NumberFormatException nfe) {
                System.out.println(STRING_KEY + " value is not a long!");
            }

            // entry gives detail about latest record of the key
            System.out.println("\n5. Get Entry");

            KeyValueEntry entry = kv.get(BYTE_KEY);
            System.out.println(BYTE_KEY + " entry: " + entry);

            entry = kv.get(STRING_KEY);
            System.out.println(STRING_KEY + " entry: " + entry);

            entry = kv.get(LONG_KEY);
            System.out.println(LONG_KEY + " entry: " + entry);

            // delete a key
            System.out.println("\n6. Delete a key");
            kv.delete(BYTE_KEY);

            // if the key has been deleted or purged, get returns null
            // watches or history will return an entry with DELETE or PURGE for the operation
            KeyValueEntry kve = kv.get(BYTE_KEY);
            System.out.println("Deleted key, result of get should be null: " + kve);

            // if the key does not exist there is no entry at all
            System.out.println("\n7. Keys does not exist");
            kve = kv.get(NOT_FOUND);
            System.out.println("Entry for " + NOT_FOUND + " should be null: " + kve);

            // Update values. You can even update a deleted key
            System.out.println("\n8.1 Update values");
            seq = kv.put(BYTE_KEY, "Byte Value 2".getBytes());
            System.out.println("Revision number should be 5, got " + seq);

            seq = kv.put(STRING_KEY, "String Value 2");
            System.out.println("Revision number should be 6, got " + seq);

            seq = kv.put(LONG_KEY, 2);
            System.out.println("Revision number should be 7, got " + seq);

            // values after updates
            System.out.println("\n8.2 Values after update");

            svalue = kv.get(BYTE_KEY).getValueAsString();
            System.out.println(BYTE_KEY + " from getValueAsString: " + svalue);

            svalue = kv.get(STRING_KEY).getValueAsString();
            System.out.println(STRING_KEY + " from getValueAsString: " + svalue);

            lvalue = kv.get(LONG_KEY).getValueAsLong();
            System.out.println(LONG_KEY + " from getValueAsLong: " + lvalue);

            // let's check the bucket info
            System.out.println("\n9.1 Bucket before update/delete");
            kvs = kvm.getStatus(exArgs.bucket);
            System.out.println(kvs);

            // update the bucket
            kvc = KeyValueConfiguration.builder(kvs.getConfiguration())
                .description(exArgs.description + "-changed")
                .maxHistoryPerKey(6)
                .build();
            kvs = kvm.update(kvc);
            System.out.println("\n9.2 Bucket after update");
            System.out.println(kvs);

            // delete the bucket
            System.out.println("\n9.3 Delete");
            kvm.delete(exArgs.bucket);

            try {
                kvm.getStatus(exArgs.bucket);
                System.out.println("UH OH! Bucket should not have been found!");
            }
            catch (JetStreamApiException e) {
                System.out.println("Bucket was not found!");
            }
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
