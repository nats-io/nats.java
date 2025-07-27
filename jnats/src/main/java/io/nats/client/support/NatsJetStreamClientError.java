// Copyright 2021-2022 The NATS Authors
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

package io.nats.client.support;

public class NatsJetStreamClientError {
    public static final int KIND_ILLEGAL_ARGUMENT = 0;
    public static final int KIND_ILLEGAL_STATE = 1;
    private static final String SUB = "SUB";
    private static final String SO = "SO";
    private static final String OS = "OS";
    private static final String CON = "CON";

    public static final NatsJetStreamClientError JsSubPullCantHaveDeliverGroup = new NatsJetStreamClientError(SUB, 90001, "Pull subscriptions can't have a deliver group.");
    public static final NatsJetStreamClientError JsSubPullCantHaveDeliverSubject = new NatsJetStreamClientError(SUB, 90002, "Pull subscriptions can't have a deliver subject.");
    public static final NatsJetStreamClientError JsSubPushCantHaveMaxPullWaiting = new NatsJetStreamClientError(SUB, 90003, "Push subscriptions cannot supply max pull waiting.");
    public static final NatsJetStreamClientError JsSubQueueDeliverGroupMismatch = new NatsJetStreamClientError(SUB, 90004, "Queue / deliver group mismatch.");
    public static final NatsJetStreamClientError JsSubFcHbNotValidPull = new NatsJetStreamClientError(SUB, 90005, "Flow Control and/or heartbeat is not valid with a pull subscription.");
    public static final NatsJetStreamClientError JsSubFcHbNotValidQueue = new NatsJetStreamClientError(SUB, 90006, "Flow Control and/or heartbeat is not valid in queue mode.");
    public static final NatsJetStreamClientError JsSubNoMatchingStreamForSubject = new NatsJetStreamClientError(SUB, 90007, "No matching streams for subject.", KIND_ILLEGAL_STATE);
    public static final NatsJetStreamClientError JsSubConsumerAlreadyConfiguredAsPush = new NatsJetStreamClientError(SUB, 90008, "Consumer is already configured as a push consumer.");
    public static final NatsJetStreamClientError JsSubConsumerAlreadyConfiguredAsPull = new NatsJetStreamClientError(SUB, 90009, "Consumer is already configured as a pull consumer.");
    public static final NatsJetStreamClientError JsSubSubjectDoesNotMatchFilter = new NatsJetStreamClientError(SUB, 90011, "Subject does not match consumer configuration filter.");
    public static final NatsJetStreamClientError JsSubConsumerAlreadyBound = new NatsJetStreamClientError(SUB, 90012, "Consumer is already bound to a subscription.");
    public static final NatsJetStreamClientError JsSubExistingConsumerNotQueue = new NatsJetStreamClientError(SUB, 90013, "Existing consumer is not configured as a queue / deliver group.");
    public static final NatsJetStreamClientError JsSubExistingConsumerIsQueue = new NatsJetStreamClientError(SUB, 90014, "Existing consumer is configured as a queue / deliver group.");
    public static final NatsJetStreamClientError JsSubExistingQueueDoesNotMatchRequestedQueue = new NatsJetStreamClientError(SUB, 90015, "Existing consumer deliver group does not match requested queue / deliver group.");
    public static final NatsJetStreamClientError JsSubExistingConsumerCannotBeModified = new NatsJetStreamClientError(SUB, 90016, "Existing consumer cannot be modified.");
    public static final NatsJetStreamClientError JsSubConsumerNotFoundRequiredInBind = new NatsJetStreamClientError(SUB, 90017, "Consumer not found, required in bind mode.");
    public static final NatsJetStreamClientError JsSubOrderedNotAllowOnQueues = new NatsJetStreamClientError(SUB, 90018, "Ordered consumer not allowed on queues.");
    public static final NatsJetStreamClientError JsSubPushCantHaveMaxBatch = new NatsJetStreamClientError(SUB, 90019, "Push subscriptions cannot supply max batch.");
    public static final NatsJetStreamClientError JsSubPushCantHaveMaxBytes = new NatsJetStreamClientError(SUB, 90020, "Push subscriptions cannot supply max bytes.");
    public static final NatsJetStreamClientError JsSubPushAsyncCantSetPending = new NatsJetStreamClientError(SUB, 90021, "Pending limits must be set directly on the dispatcher.");
    public static final NatsJetStreamClientError JsSubSubjectNeededToLookupStream = new NatsJetStreamClientError(SUB, 90022, "Subject needed to lookup stream. Provide either a subscribe subject or a ConsumerConfiguration filter subject.");

    public static final NatsJetStreamClientError JsSoDurableMismatch = new NatsJetStreamClientError(SO, 90101, "Builder durable must match the consumer configuration durable if both are provided.");
    public static final NatsJetStreamClientError JsSoDeliverGroupMismatch = new NatsJetStreamClientError(SO, 90102, "Builder deliver group must match the consumer configuration deliver group if both are provided.");
    public static final NatsJetStreamClientError JsSoDeliverSubjectMismatch = new NatsJetStreamClientError(SO, 90103, "Builder deliver subject must match the consumer configuration deliver subject if both are provided.");
    public static final NatsJetStreamClientError JsSoOrderedNotAllowedWithBind = new NatsJetStreamClientError(SO, 90104, "Bind is not allowed with an ordered consumer.");
    public static final NatsJetStreamClientError JsSoOrderedNotAllowedWithDeliverGroup = new NatsJetStreamClientError(SO, 90105, "Deliver group is not allowed with an ordered consumer.");
    public static final NatsJetStreamClientError JsSoOrderedNotAllowedWithDurable = new NatsJetStreamClientError(SO, 90106, "Durable is not allowed with an ordered consumer.");
    public static final NatsJetStreamClientError JsSoOrderedNotAllowedWithDeliverSubject = new NatsJetStreamClientError(SO, 90107, "Deliver subject is not allowed with an ordered consumer.");
    public static final NatsJetStreamClientError JsSoOrderedRequiresAckPolicyNone = new NatsJetStreamClientError(SO, 90108, "Ordered consumer requires Ack Policy None.");
    public static final NatsJetStreamClientError JsSoOrderedRequiresMaxDeliverOfOne = new NatsJetStreamClientError(SO, 90109, "Max Deliver is limited to 1 with an ordered consumer.");
    public static final NatsJetStreamClientError JsSoNameMismatch = new NatsJetStreamClientError(SO, 90110, "Builder name must match the consumer configuration name if both are provided.");
    public static final NatsJetStreamClientError JsSoOrderedMemStorageNotSuppliedOrTrue = new NatsJetStreamClientError(SO, 90111, "Mem Storage must be true if supplied.");
    public static final NatsJetStreamClientError JsSoOrderedReplicasNotSuppliedOrOne = new NatsJetStreamClientError(SO, 90112, "Replicas must be 1 if supplied.");
    public static final NatsJetStreamClientError JsSoNameOrDurableRequiredForBind = new NatsJetStreamClientError(SO, 90113, "Name or Durable required for Bind.");

    public static final NatsJetStreamClientError OsObjectNotFound = new NatsJetStreamClientError(OS, 90201, "The object was not found.");
    public static final NatsJetStreamClientError OsObjectIsDeleted = new NatsJetStreamClientError(OS, 90202, "The object is deleted.");
    public static final NatsJetStreamClientError OsObjectAlreadyExists = new NatsJetStreamClientError(OS, 90203, "An object with that name already exists.");
    public static final NatsJetStreamClientError OsCantLinkToLink = new NatsJetStreamClientError(OS, 90204, "A link cannot link to another link.");
    public static final NatsJetStreamClientError OsGetDigestMismatch = new NatsJetStreamClientError(OS, 90205, "Digest does not match meta data.");
    public static final NatsJetStreamClientError OsGetChunksMismatch = new NatsJetStreamClientError(OS, 90206, "Number of chunks does not match meta data.");
    public static final NatsJetStreamClientError OsGetSizeMismatch = new NatsJetStreamClientError(OS, 90207, "Total size does not match meta data.");
    public static final NatsJetStreamClientError OsGetLinkToBucket = new NatsJetStreamClientError(OS, 90208, "Cannot get object, it is a link to a bucket.");
    public static final NatsJetStreamClientError OsLinkNotAllowOnPut = new NatsJetStreamClientError(OS, 90209, "Link not allowed in metadata when putting an object.");

    public static final NatsJetStreamClientError JsConsumerCreate290NotAvailable = new NatsJetStreamClientError(CON, 90301, "Name field not valid when v2.9.0 consumer create api is not available.");
    public static final NatsJetStreamClientError JsConsumerNameDurableMismatch = new NatsJetStreamClientError(CON, 90302, "Name must match durable if both are supplied.");
    public static final NatsJetStreamClientError JsMultipleFilterSubjects210NotAvailable = new NatsJetStreamClientError(CON, 90303, "Multiple filter subjects not available until server version 2.10.0.");

    @Deprecated // Fixed spelling error
    public static final NatsJetStreamClientError JsSubFcHbHbNotValidQueue = new NatsJetStreamClientError(SUB, 90006, "Flow Control and/or heartbeat is not valid in queue mode.");

    @Deprecated // More comprehensive name
    public static final NatsJetStreamClientError JsConsumerCantUseNameBefore290 = new NatsJetStreamClientError(CON, 90301, "Name field not valid against pre v2.9.0 servers.");

    @Deprecated // More comprehensive name
    public static final NatsJetStreamClientError JsSoOrderedRequiresMaxDeliver = new NatsJetStreamClientError(SO, 90109, "Max Deliver is limited to 1 with an ordered consumer.");

    private final String id;
    private final String message;
    private final int kind;

    public NatsJetStreamClientError(String group, int code, String description) {
        this(group, code, description, KIND_ILLEGAL_ARGUMENT);
    }

    public NatsJetStreamClientError(String group, int code, String description, int kind) {
        id = String.format("%s-%d", group, code);
        message = String.format("[%s] %s", id, description);
        this.kind = kind;
    }

    public RuntimeException instance() {
        return _instance(message);
    }

    public RuntimeException instance(String extraMessage) {
        return _instance(message + " " + extraMessage);
    }

    private RuntimeException _instance(String msg) {
        if (kind == KIND_ILLEGAL_ARGUMENT) {
            return new IllegalArgumentException(msg);
        }
        return new IllegalStateException(msg);
    }

    public String id() {
        return id;
    }

    public String message() {
        return message;
    }

    public int getKind() {
        return kind;
    }
}
