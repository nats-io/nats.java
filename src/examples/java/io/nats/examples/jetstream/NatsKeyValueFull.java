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
@SuppressWarnings("ImplicitArrayToString")
public class NatsKeyValueFull {
    static final String usageString =
            "\nUsage: java -cp <classpath> NatsJsManageStreams [-s server]"
                    + "\n\nUse tls:// or opentls:// to require tls, via the Default SSLContext\n"
                    + "\nSet the environment variable NATS_NKEY to use challenge response authentication by setting a file containing your private key.\n"
                    + "\nSet the environment variable NATS_CREDS to use JWT/NKey authentication by setting a file containing your user creds.\n"
                    + "\nUse the URL in the -s server parameter for user/pass/token authentication.\n";

    private static final String BYTE_KEY = "byteKey";
    private static final String STRING_KEY = "stringKey";
    private static final String LONG_KEY = "longKey";
    private static final String NOT_FOUND = "notFound";

    private static final String BUCKET_NAME = "buk"; // "example-bucket-" + uniqueEnough();

    public static void main(String[] args) {
        ExampleArgs exArgs = ExampleUtils.optionalServer(args, usageString);

        try (Connection nc = Nats.connect(ExampleUtils.createExampleOptions(exArgs.server))) {
            // get the kv management context
            KeyValueManagement kvm = nc.keyValueManagement();

            // create the bucket
            KeyValueConfiguration bc = KeyValueConfiguration.builder()
                    .name(BUCKET_NAME)
                    .maxHistoryPerKey(5)
                    .storageType(StorageType.Memory)
                    .build();

            KeyValueStatus bi = kvm.create(bc);
            System.out.println(bi);

            // get the kv context for the specific bucket
            KeyValue kv = nc.keyValue(BUCKET_NAME);

            // Put some keys. Each key is put in a subject in the bucket (stream)
            // The put returns the sequence number in the bucket (stream)
            System.out.println("\n1. Put");

            long seq = kv.put(BYTE_KEY, "Byte Value 1".getBytes());
            System.out.println("Sequence Number should be 1, got " + seq);

            seq = kv.put(STRING_KEY, "String Value 1");
            System.out.println("Sequence Number should be 2, got " + seq);

            seq = kv.put(LONG_KEY, 1);
            System.out.println("Sequence Number should be 3, got " + seq);

            // retrieve the values. all types are stored as bytes
            // so you can always get the bytes directly
            System.out.println("\n2. Get Value (Bytes)");

            byte[] bvalue = kv.getValue(BYTE_KEY);
            System.out.println(BYTE_KEY + " from getValue: " + new String(bvalue));

            bvalue = kv.getValue(STRING_KEY);
            System.out.println(STRING_KEY + " from getValue: " + new String(bvalue));

            bvalue = kv.getValue(LONG_KEY);
            System.out.println(LONG_KEY + " from getValue: " + new String(bvalue));

            // if you know the value is not binary and can safely be read
            // as a UTF-8 string, the getStringValue method is ok to use
            System.out.println("\n3. Get String Value");

            String svalue = kv.getStringValue(BYTE_KEY);
            System.out.println(BYTE_KEY + " from getStringValue: " + svalue);

            svalue = kv.getStringValue(STRING_KEY);
            System.out.println(STRING_KEY + " from getStringValue: " + svalue);

            svalue = kv.getStringValue(LONG_KEY);
            System.out.println(LONG_KEY + " from getStringValue: " + svalue);

            // if you know the value is a long, you can use
            // the getLongValue method
            // if it's not a number a NumberFormatException is thrown
            System.out.println("\n4. Get Long Value");

            Long lvalue = kv.getLongValue(LONG_KEY);
            System.out.println(LONG_KEY + " from getLongValue: " + lvalue);

            try {
                kv.getLongValue(STRING_KEY);
            }
            catch (NumberFormatException nfe) {
                System.out.println(STRING_KEY + " value is not a long!");
            }

            // entry gives detail about latest record of the key
            System.out.println("\n5. Get Entry");

            KeyValueEntry entry = kv.getEntry(BYTE_KEY);
            System.out.println(BYTE_KEY + " entry: " + entry);

            entry = kv.getEntry(STRING_KEY);
            System.out.println(STRING_KEY + " entry: " + entry);

            entry = kv.getEntry(LONG_KEY);
            System.out.println(LONG_KEY + " entry: " + entry);

            // delete a key
            System.out.println("\n6. Delete a key");
            kv.delete(BYTE_KEY);
            System.out.println("Sequence Number should be 4, got " + seq);

            // it's value is now null
            bvalue = kv.getValue(BYTE_KEY);
            System.out.println("Deleted value should be null: " + (bvalue == null));

            // but it's entry still exists
            entry = kv.getEntry(BYTE_KEY);
            System.out.println("Deleted " + BYTE_KEY + " entry: " + entry);

            // if the key has been deleted or not found / never existed
            // all varieties of get will return null
            System.out.println("\n7. Keys not found");
            bvalue = kv.getValue(BYTE_KEY);
            System.out.println("Should be null: " + bvalue);

            svalue = kv.getStringValue(BYTE_KEY);
            System.out.println("Should be null: " + svalue);

            lvalue = kv.getLongValue(BYTE_KEY);
            System.out.println("Should be null: " + lvalue);

            bvalue = kv.getValue(NOT_FOUND);
            System.out.println("Should be null: " + bvalue);

            svalue = kv.getStringValue(NOT_FOUND);
            System.out.println("Should be null: " + svalue);

            lvalue = kv.getLongValue(NOT_FOUND);
            System.out.println("Should be null: " + lvalue);

            // Update values. You can even update a deleted key
            System.out.println("\n8.1 Update values");
            seq = kv.put(BYTE_KEY, "Byte Value 2".getBytes());
            System.out.println("Sequence Number should be 5, got " + seq);

            seq = kv.put(STRING_KEY, "String Value 2");
            System.out.println("Sequence Number should be 6, got " + seq);

            seq = kv.put(LONG_KEY, 2);
            System.out.println("Sequence Number should be 7, got " + seq);

            // values after updates
            System.out.println("\n8.2 Values after update");

            svalue = kv.getStringValue(BYTE_KEY);
            System.out.println(BYTE_KEY + " from getStringValue: " + svalue);

            svalue = kv.getStringValue(STRING_KEY);
            System.out.println(STRING_KEY + " from getStringValue: " + svalue);

            lvalue = kv.getLongValue(LONG_KEY);
            System.out.println(LONG_KEY + " from getLongValue: " + svalue);

            // let's check the bucket info
            System.out.println("\n9.1 Bucket before delete");
            bi = kvm.getBucketInfo(BUCKET_NAME);
            System.out.println(bi);

            // delete the bucket
            System.out.println("\n9.2 Delete");
            kvm.delete(BUCKET_NAME);

            try {
                kvm.getBucketInfo(BUCKET_NAME);
                System.out.println("UH OH! Stream for bucket should not have been found!");
            }
            catch (JetStreamApiException e) {
                System.out.println("Stream for bucket was not found!");
            }
        }
        catch (Exception exp) {
            exp.printStackTrace();
        }
    }
}
