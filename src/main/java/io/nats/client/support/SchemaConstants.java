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

package io.nats.client.support;

import java.util.regex.Pattern;

import static io.nats.client.support.JsonUtils.*;

public interface SchemaConstants {

    String ACK_FLOOR = "ack_floor";
    String ACK_POLICY = "ack_policy";
    String ACK_WAIT = "ack_wait";
    String ACTIVE = "active";
    String API = "api";
    String AUTH = "auth_required";
    String BYTES = "bytes";
    String CLIENT_ID = "client_id";
    String CLIENT_IP = "client_ip";
    String CLUSTER = "cluster";
    String CODE = "code";
    String CONFIG = "config";
    String CONNECT_URLS = "connect_urls";
    String CONSUMER_COUNT = "consumer_count";
    String CONSUMER_SEQ = "consumer_seq";
    String CONSUMERS = "consumers";
    String CREATED = "created";
    String CURRENT = "current";
    String DATA = "data";
    String DELIVER = "deliver";
    String DELIVER_POLICY = "deliver_policy";
    String DELIVER_SUBJECT = "deliver_subject";
    String DELIVERED = "delivered";
    String DESCRIPTION = "description";
    String DISCARD = "discard";
    String DUPLICATE = "duplicate";
    String DUPLICATE_WINDOW = "duplicate_window";
    String DURABLE_NAME = "durable_name";
    String EXTERNAL = "external";
    String FILTER_SUBJECT = "filter_subject";
    String FIRST_SEQ = "first_seq";
    String FIRST_TS = "first_ts";
    String GO = "go";
    String HDRS = "hdrs";
    String HEADERS = "headers";
    String HOST = "host";
    String JETSTREAM = "jetstream";
    String LAG = "lag";
    String LAME_DUCK_MODE = "ldm";
    String LAST_SEQ = "last_seq";
    String LAST_TS = "last_ts";
    String LEADER = "leader";
    String LIMIT = "limit";
    String MAX_ACK_PENDING = "max_ack_pending";
    String MAX_AGE = "max_age";
    String MAX_BYTES = "max_bytes";
    String MAX_CONSUMERS = "max_consumers";
    String MAX_DELIVER = "max_deliver";
    String MAX_MEMORY = "max_memory";
    String MAX_MSG_SIZE = "max_msg_size";
    String MAX_MSGS = "max_msgs";
    String MAX_PAYLOAD = "max_payload";
    String MAX_STORAGE = "max_storage";
    String MAX_STREAMS = "max_streams";
    String MEMORY = "memory";
    String MESSAGES = "messages";
    String MIRROR = "mirror";
    String NAME = "name";
    String NO_ACK = "no_ack";
    String NONCE = "nonce";
    String NUM_ACK_PENDING = "num_ack_pending";
    String NUM_PENDING = "num_pending";
    String NUM_REDELIVERED = "num_redelivered";
    String NUM_REPLICAS = "num_replicas";
    String NUM_WAITING = "num_waiting";
    String OFFLINE = "offline";
    String OFFSET = "offset";
    String OPT_START_SEQ = "opt_start_seq";
    String OPT_START_TIME = "opt_start_time";
    String PLACEMENT = "placement";
    String PORT = "port";
    String PROTOCOL_VERSION = "proto";
    String RATE_LIMIT = "rate_limit";
    String REPLAY_POLICY = "replay_policy";
    String REPLICAS = "replicas";
    String RETENTION = "retention";
    String SAMPLE_FREQ = "sample_freq";
    String SEQ = "seq";
    String SERVER_ID = "server_id";
    String SERVER_NAME = "server_name";
    String SOURCE = "source";
    String SOURCES = "sources";
    String STATE = "state";
    String STORAGE = "storage";
    String STREAM_NAME = "stream_name";
    String STREAM_SEQ = "stream_seq";
    String STREAM = "stream";
    String STREAMS = "streams";
    String SUBJECT = "subject";
    String SUBJECTS = "subjects";
    String SUCCESS = "success";
    String TAGS = "tags";
    String TEMPLATE = "template";
    String TIME = "time";
    String TLS = "tls_required";
    String TOTAL = "total";
    String TYPE = "type";
    String VERSION = "version";

    Pattern ACK_POLICY_RE = buildPattern(ACK_POLICY, FieldType.jsonString);
    Pattern ACK_WAIT_RE = buildPattern(ACK_WAIT, FieldType.jsonNumber);
    Pattern ACTIVE_RE = buildPattern(ACTIVE, FieldType.jsonNumber);
    Pattern API_RE = buildPattern(API, FieldType.jsonString);
    Pattern BYTES_RE = buildPattern(BYTES, FieldType.jsonNumber);
    Pattern CLUSTER_RE = buildPattern(CLUSTER, FieldType.jsonString);
    Pattern CODE_RE = buildPattern(CODE, FieldType.jsonNumber);
    Pattern CONSUMER_COUNT_RE = buildPattern(CONSUMER_COUNT, FieldType.jsonNumber);
    Pattern CONSUMER_SEQ_RE = buildPattern(CONSUMER_SEQ, FieldType.jsonNumber);
    Pattern CONSUMERS_RE = buildNumberPattern(CONSUMERS);
    Pattern CURRENT_RE = JsonUtils.buildPattern(CURRENT, FieldType.jsonBoolean);
    Pattern CREATED_RE = buildPattern(CREATED, FieldType.jsonString);
    Pattern DATA_RE = buildPattern(DATA, FieldType.jsonString);
    Pattern DELIVER_RE = buildPattern(DELIVER, FieldType.jsonString);
    Pattern DELIVER_POLICY_RE = buildPattern(DELIVER_POLICY, FieldType.jsonString);
    Pattern DELIVER_SUBJECT_RE = buildPattern(DELIVER_SUBJECT, FieldType.jsonString);
    Pattern DESCRIPTION_RE = buildPattern(DESCRIPTION, FieldType.jsonString);
    Pattern DISCARD_RE = buildPattern(DISCARD, FieldType.jsonString);
    Pattern DUPLICATE_RE = JsonUtils.buildPattern(DUPLICATE, FieldType.jsonBoolean);
    Pattern DUPLICATE_WINDOW_RE = buildPattern(DUPLICATE_WINDOW, FieldType.jsonNumber);
    Pattern DURABLE_NAME_RE = buildPattern(DURABLE_NAME, FieldType.jsonString);
    Pattern FILTER_SUBJECT_RE = buildPattern(FILTER_SUBJECT, FieldType.jsonString);
    Pattern FIRST_SEQ_RE = buildPattern(FIRST_SEQ, FieldType.jsonNumber);
    Pattern FIRST_TS_RE = buildPattern(FIRST_TS, FieldType.jsonString);
    Pattern HDRS_RE = buildPattern(HDRS, FieldType.jsonString);
    Pattern LAG_RE = buildPattern(LAG, FieldType.jsonNumber);
    Pattern LAST_SEQ_RE = buildPattern(LAST_SEQ, FieldType.jsonNumber);
    Pattern LAST_TS_RE = buildPattern(LAST_TS, FieldType.jsonString);
    Pattern LEADER_RE = buildPattern(LEADER, FieldType.jsonString);
    Pattern LIMIT_RE = JsonUtils.buildPattern(LIMIT, JsonUtils.FieldType.jsonNumber);
    Pattern MAX_ACK_PENDING_RE = buildPattern(MAX_ACK_PENDING, FieldType.jsonNumber);
    Pattern MAX_AGE_RE = buildPattern(MAX_AGE, FieldType.jsonNumber);
    Pattern MAX_BYTES_RE = buildPattern(MAX_BYTES, FieldType.jsonNumber);
    Pattern MAX_CONSUMERS_RE = buildPattern(MAX_CONSUMERS, FieldType.jsonNumber);
    Pattern MAX_DELIVER_RE = buildPattern(MAX_DELIVER, FieldType.jsonNumber);
    Pattern MAX_MEMORY_RE = buildNumberPattern(MAX_MEMORY);
    Pattern MAX_MSG_SIZE_RE = buildPattern(MAX_MSG_SIZE, FieldType.jsonNumber);
    Pattern MAX_MSGS_RE = buildPattern(MAX_MSGS, FieldType.jsonNumber);
    Pattern MAX_STORAGE_RE = buildNumberPattern(MAX_STORAGE);
    Pattern MAX_STREAMS_RE = buildNumberPattern(MAX_STREAMS);
    Pattern MEMORY_RE = buildNumberPattern(MEMORY);
    Pattern MESSAGES_RE = buildPattern(MESSAGES, FieldType.jsonNumber);
    Pattern NAME_RE = buildPattern(NAME, FieldType.jsonString);
    Pattern NO_ACK_RE = buildPattern(NO_ACK, FieldType.jsonBoolean);
    Pattern NUM_ACK_PENDING_RE = buildPattern(NUM_ACK_PENDING, FieldType.jsonNumber);
    Pattern NUM_PENDING_RE = buildPattern(NUM_PENDING, FieldType.jsonNumber);
    Pattern NUM_REDELIVERED_RE = buildPattern(NUM_REDELIVERED, FieldType.jsonNumber);
    Pattern NUM_WAITING_RE = buildPattern(NUM_WAITING, FieldType.jsonNumber);
    Pattern OFFLINE_RE = JsonUtils.buildPattern(OFFLINE, FieldType.jsonBoolean);
    Pattern OFFSET_RE = JsonUtils.buildPattern(OFFSET, JsonUtils.FieldType.jsonNumber);
    Pattern OPT_START_SEQ_RE = buildPattern(OPT_START_SEQ, FieldType.jsonNumber);
    Pattern OPT_START_TIME_RE = buildPattern(OPT_START_TIME, FieldType.jsonString);
    Pattern RATE_LIMIT_RE = buildPattern(RATE_LIMIT, FieldType.jsonNumber);
    Pattern REPLAY_POLICY_RE = buildPattern(REPLAY_POLICY, FieldType.jsonString);
    Pattern REPLICAS_RE = buildPattern(NUM_REPLICAS, FieldType.jsonNumber);
    Pattern RETENTION_RE = buildPattern(RETENTION, FieldType.jsonString);
    Pattern SAMPLE_FREQ_RE = buildPattern(SAMPLE_FREQ, FieldType.jsonString);
    Pattern SEQ_RE = buildPattern(SEQ, FieldType.jsonNumber);
    Pattern STORAGE_RE = buildNumberPattern(STORAGE);
    Pattern STORAGE_TYPE_RE = buildPattern(STORAGE, FieldType.jsonString);
    Pattern STREAM_NAME_RE = buildPattern(STREAM_NAME, FieldType.jsonString);
    Pattern STREAM_SEQ_RE = buildPattern(STREAM_SEQ, FieldType.jsonNumber);
    Pattern STREAM_RE = buildStringPattern(STREAM);
    Pattern STREAMS_RE = buildNumberPattern(STREAMS);
    Pattern SUBJECT_RE = buildStringPattern(SUBJECT);
    Pattern SUCCESS_RE = buildBooleanPattern(SUCCESS);
    Pattern TEMPLATE_RE = buildPattern(TEMPLATE, FieldType.jsonString);
    Pattern TIME_RE = buildPattern(TIME, FieldType.jsonString);
    Pattern TOTAL_RE = buildPattern(TOTAL, JsonUtils.FieldType.jsonNumber);
    Pattern TYPE_RE = buildPattern(TYPE, FieldType.jsonString);
}
