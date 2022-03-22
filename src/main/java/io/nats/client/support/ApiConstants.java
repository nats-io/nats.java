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

public interface ApiConstants {

    String ACK_FLOOR         = "ack_floor";
    String ACK_POLICY        = "ack_policy";
    String ACK_WAIT          = "ack_wait";
    String ACTIVE            = "active";
    String ALLOW_ROLLUP_HDRS = "allow_rollup_hdrs";
    String API               = "api";
    String AUTH_REQUIRED     = "auth_required";
    String BACKOFF           = "backoff";
    String BYTES             = "bytes";
    String CLIENT_ID         = "client_id";
    String CLIENT_IP         = "client_ip";
    String CLUSTER           = "cluster";
    String CODE              = "code";
    String CONFIG            = "config";
    String CONNECT_URLS      = "connect_urls";
    String CONSUMER_COUNT    = "consumer_count";
    String CONSUMER_SEQ      = "consumer_seq";
    String CONSUMERS         = "consumers";
    String CREATED           = "created";
    String CURRENT           = "current";
    String DATA              = "data";
    String DELETED           = "deleted";
    String DELETED_DETAILS   = "deleted_details";
    String DELIVER           = "deliver";
    String DELIVER_GROUP     = "deliver_group";
    String DELIVER_POLICY    = "deliver_policy";
    String DELIVER_SUBJECT   = "deliver_subject";
    String DELIVERED         = "delivered";
    String DENY_DELETE       = "deny_delete";
    String DENY_PURGE        = "deny_purge";
    String DESCRIPTION       = "description";
    String DISCARD           = "discard";
    String DOMAIN            = "domain";
    String DUPLICATE         = "duplicate";
    String DUPLICATE_WINDOW  = "duplicate_window";
    String DURABLE_NAME      = "durable_name";
    String ERR_CODE          = "err_code";
    String ERROR             = "error";
    String EXTERNAL          = "external";
    String FILTER            = "filter";
    String FILTER_SUBJECT    = "filter_subject";
    String FIRST_SEQ         = "first_seq";
    String FIRST_TS          = "first_ts";
    String FLOW_CONTROL      = "flow_control";
    String GO                = "go";
    String HDRS              = "hdrs";
    String HEADERS           = "headers";
    String HEADERS_ONLY      = "headers_only";
    String HOST              = "host";
    String IDLE_HEARTBEAT    = "idle_heartbeat";
    String INACTIVE_THRESHOLD= "inactive_threshold";
    String JETSTREAM         = "jetstream";
    String KEEP              = "keep";
    String LAG               = "lag";
    String LAME_DUCK_MODE    = "ldm";
    String LAST_BY_SUBJECT   = "last_by_subj";
    String LAST_SEQ          = "last_seq";
    String LAST_TS           = "last_ts";
    String LEADER            = "leader";
    String LIMIT             = "limit";
    String MAX_ACK_PENDING   = "max_ack_pending";
    String MAX_AGE           = "max_age";
    String MAX_BATCH         = "max_batch";
    String MAX_BYTES         = "max_bytes";
    String MAX_CONSUMERS     = "max_consumers";
    String MAX_DELIVER       = "max_deliver";
    String MAX_EXPIRES       = "max_expires";
    String MAX_MEMORY        = "max_memory";
    String MAX_MSG_SIZE      = "max_msg_size";
    String MAX_MSGS          = "max_msgs";
    String MAX_MSGS_PER_SUB  = "max_msgs_per_subject";
    String MAX_PAYLOAD       = "max_payload";
    String MAX_STORAGE       = "max_storage";
    String MAX_STREAMS       = "max_streams";
    String MAX_WAITING       = "max_waiting"; // this is correct! the meaning name is different than the field name
    String MEMORY            = "memory";
    String MESSAGES          = "messages";
    String MIRROR            = "mirror";
    String NAME              = "name";
    String NO_ACK            = "no_ack";
    String NONCE             = "nonce";
    String NUM_ACK_PENDING   = "num_ack_pending";
    String NUM_DELETED       = "num_deleted";
    String NUM_PENDING       = "num_pending";
    String NUM_REDELIVERED   = "num_redelivered";
    String NUM_REPLICAS      = "num_replicas";
    String NUM_SUBJECTS      = "num_subjects";
    String NUM_WAITING       = "num_waiting";
    String OFFLINE           = "offline";
    String OFFSET            = "offset";
    String OPT_START_SEQ     = "opt_start_seq";
    String OPT_START_TIME    = "opt_start_time";
    String PLACEMENT         = "placement";
    String PORT              = "port";
    String PROTO             = "proto";
    String PURGED            = "purged";
    String PUSH_BOUND        = "push_bound";
    String RATE_LIMIT_BPS    = "rate_limit_bps";
    String REPLAY_POLICY     = "replay_policy";
    String REPLICA           = "replica";
    String REPLICAS          = "replicas";
    String RETENTION         = "retention";
    String SAMPLE_FREQ       = "sample_freq";
    String SEALED            = "sealed";
    String SEQ               = "seq";
    String SERVER_ID         = "server_id";
    String SERVER_NAME       = "server_name";
    String SOURCE            = "source";
    String SOURCES           = "sources";
    String STATE             = "state";
    String STORAGE           = "storage";
    String STREAM_NAME       = "stream_name";
    String STREAM_SEQ        = "stream_seq";
    String STREAM            = "stream";
    String STREAMS           = "streams";
    String SUBJECT           = "subject";
    String SUBJECTS          = "subjects";
    String SUBJECTS_FILTER   = "subjects_filter";
    String SUCCESS           = "success";
    String TAGS              = "tags";
    String TEMPLATE_OWNER    = "template_owner";
    String TIME              = "time";
    String TLS               = "tls_required";
    String TOTAL             = "total";
    String TYPE              = "type";
    String VERSION           = "version";

    Pattern ACK_POLICY_RE         = string_pattern(ACK_POLICY);
    Pattern ACK_WAIT_RE           = integer_pattern(ACK_WAIT);
    Pattern ACTIVE_RE             = integer_pattern(ACTIVE);
    Pattern ALLOW_ROLLUP_HDRS_RE  = boolean_pattern(ALLOW_ROLLUP_HDRS);
    Pattern API_RE                = string_pattern(API);
    Pattern AUTH_REQUIRED_RE      = boolean_pattern(AUTH_REQUIRED);
    Pattern BYTES_RE              = integer_pattern(BYTES);
    Pattern CLIENT_ID_RE          = integer_pattern(CLIENT_ID);
    Pattern CLIENT_IP_RE          = string_pattern(CLIENT_IP);
    Pattern CLUSTER_RE            = string_pattern(CLUSTER);
    Pattern CODE_RE               = integer_pattern(CODE);
    Pattern CONSUMER_COUNT_RE     = integer_pattern(CONSUMER_COUNT);
    Pattern CONSUMER_SEQ_RE       = integer_pattern(CONSUMER_SEQ);
    Pattern CONSUMERS_RE          = integer_pattern(CONSUMERS);
    Pattern CURRENT_RE            = boolean_pattern(CURRENT);
    Pattern CREATED_RE            = string_pattern(CREATED);
    Pattern DATA_RE               = string_pattern(DATA);
    Pattern DELIVER_RE            = string_pattern(DELIVER);
    Pattern DELIVER_GROUP_RE      = string_pattern(DELIVER_GROUP);
    Pattern DELIVER_POLICY_RE     = string_pattern(DELIVER_POLICY);
    Pattern DELIVER_SUBJECT_RE    = string_pattern(DELIVER_SUBJECT);
    Pattern DENY_DELETE_RE        = boolean_pattern(DENY_DELETE);
    Pattern DENY_PURGE_RE         = boolean_pattern(DENY_PURGE);
    Pattern DESCRIPTION_RE        = string_pattern(DESCRIPTION);
    Pattern DISCARD_RE            = string_pattern(DISCARD);
    Pattern DOMAIN_RE             = string_pattern(DOMAIN);
    Pattern DUPLICATE_RE          = boolean_pattern(DUPLICATE);
    Pattern DUPLICATE_WINDOW_RE   = integer_pattern(DUPLICATE_WINDOW);
    Pattern DURABLE_NAME_RE       = string_pattern(DURABLE_NAME);
    Pattern ERR_CODE_RE           = integer_pattern(ERR_CODE);
    Pattern FILTER_SUBJECT_RE     = string_pattern(FILTER_SUBJECT);
    Pattern FIRST_SEQ_RE          = integer_pattern(FIRST_SEQ);
    Pattern FIRST_TS_RE           = string_pattern(FIRST_TS);
    Pattern FLOW_CONTROL_RE       = boolean_pattern(FLOW_CONTROL);
    Pattern GO_RE                 = string_pattern(GO);
    Pattern HDRS_RE               = string_pattern(HDRS);
    Pattern HEADERS_RE            = boolean_pattern(HEADERS);
    Pattern HEADERS_ONLY_RE       = boolean_pattern(HEADERS_ONLY);
    Pattern HOST_RE               = string_pattern(HOST);
    Pattern IDLE_HEARTBEAT_RE     = integer_pattern(IDLE_HEARTBEAT);
    Pattern INACTIVE_THRESHOLD_RE = integer_pattern(INACTIVE_THRESHOLD);
    Pattern JET_STREAM_RE         = boolean_pattern(JETSTREAM);
    Pattern LAG_RE                = integer_pattern(LAG);
    Pattern LAME_DUCK_MODE_RE     = boolean_pattern(LAME_DUCK_MODE);
    Pattern LAST_SEQ_RE           = integer_pattern(LAST_SEQ);
    Pattern LAST_TS_RE            = string_pattern(LAST_TS);
    Pattern LEADER_RE             = string_pattern(LEADER);
    Pattern LIMIT_RE              = integer_pattern(LIMIT);
    Pattern MAX_ACK_PENDING_RE    = integer_pattern(MAX_ACK_PENDING);
    Pattern MAX_AGE_RE            = integer_pattern(MAX_AGE);
    Pattern MAX_BATCH_RE          = integer_pattern(MAX_BATCH);
    Pattern MAX_BYTES_RE          = integer_pattern(MAX_BYTES);
    Pattern MAX_CONSUMERS_RE      = integer_pattern(MAX_CONSUMERS);
    Pattern MAX_DELIVER_RE        = integer_pattern(MAX_DELIVER);
    Pattern MAX_EXPIRES_RE        = integer_pattern(MAX_EXPIRES);
    Pattern MAX_MEMORY_RE         = integer_pattern(MAX_MEMORY);
    Pattern MAX_MSG_SIZE_RE       = integer_pattern(MAX_MSG_SIZE);
    Pattern MAX_MSGS_RE           = integer_pattern(MAX_MSGS);
    Pattern MAX_MSGS_PER_SUB_RE   = integer_pattern(MAX_MSGS_PER_SUB);
    Pattern MAX_PAYLOAD_RE        = integer_pattern(MAX_PAYLOAD);
    Pattern MAX_STORAGE_RE        = integer_pattern(MAX_STORAGE);
    Pattern MAX_STREAMS_RE        = integer_pattern(MAX_STREAMS);
    Pattern MAX_WAITING_RE        = integer_pattern(MAX_WAITING);
    Pattern MEMORY_RE             = integer_pattern(MEMORY);
    Pattern MESSAGES_RE           = integer_pattern(MESSAGES);
    Pattern NAME_RE               = string_pattern(NAME);
    Pattern NO_ACK_RE             = boolean_pattern(NO_ACK);
    Pattern NONCE_RE              = string_pattern(NONCE);
    Pattern NUM_ACK_PENDING_RE    = integer_pattern(NUM_ACK_PENDING);
    Pattern NUM_DELETED_RE        = integer_pattern(NUM_DELETED);
    Pattern NUM_PENDING_RE        = integer_pattern(NUM_PENDING);
    Pattern NUM_REDELIVERED_RE    = integer_pattern(NUM_REDELIVERED);
    Pattern NUM_REPLICAS_RE       = integer_pattern(NUM_REPLICAS);
    Pattern NUM_SUBJECTS_RE       = integer_pattern(NUM_SUBJECTS);
    Pattern NUM_WAITING_RE        = integer_pattern(NUM_WAITING);
    Pattern OFFLINE_RE            = boolean_pattern(OFFLINE);
    Pattern OFFSET_RE             = integer_pattern(OFFSET);
    Pattern OPT_START_SEQ_RE      = integer_pattern(OPT_START_SEQ);
    Pattern OPT_START_TIME_RE     = string_pattern(OPT_START_TIME);
    Pattern PORT_RE               = integer_pattern(PORT);
    Pattern PROTO_RE              = integer_pattern(PROTO);
    Pattern PURGED_RE             = integer_pattern(PURGED);
    Pattern PUSH_BOUND_RE         = boolean_pattern(PUSH_BOUND);
    Pattern RATE_LIMIT_BPS_RE     = integer_pattern(RATE_LIMIT_BPS);
    Pattern REPLAY_POLICY_RE      = string_pattern(REPLAY_POLICY);
    Pattern RETENTION_RE          = string_pattern(RETENTION);
    Pattern SAMPLE_FREQ_RE        = string_pattern(SAMPLE_FREQ);
    Pattern SEALED_RE             = boolean_pattern(SEALED);
    Pattern SEQ_RE                = integer_pattern(SEQ);
    Pattern SERVER_ID_RE          = string_pattern(SERVER_ID);
    Pattern SERVER_NAME_RE        = string_pattern(SERVER_NAME);
    Pattern STORAGE_RE            = integer_pattern(STORAGE);
    Pattern STORAGE_TYPE_RE       = string_pattern(STORAGE);
    Pattern STREAM_NAME_RE        = string_pattern(STREAM_NAME);
    Pattern STREAM_SEQ_RE         = integer_pattern(STREAM_SEQ);
    Pattern STREAM_RE             = string_pattern(STREAM);
    Pattern STREAMS_RE            = integer_pattern(STREAMS);
    Pattern SUBJECT_RE            = string_pattern(SUBJECT);
    Pattern SUCCESS_RE            = boolean_pattern(SUCCESS);
    Pattern TEMPLATE_OWNER_RE     = string_pattern(TEMPLATE_OWNER);
    Pattern TIME_RE               = string_pattern(TIME);
    Pattern TLS_RE                = boolean_pattern(TLS);
    Pattern TOTAL_RE              = integer_pattern(TOTAL);
    Pattern TYPE_RE               = string_pattern(TYPE);
    Pattern VERSION_RE            = string_pattern(VERSION);
}
