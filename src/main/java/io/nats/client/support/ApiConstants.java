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

/**
 * Constants covering server api schema fields
 */
public interface ApiConstants {
    /** ack_floor */                 String ACK_FLOOR                     = "ack_floor";
    /** ack_policy */                String ACK_POLICY                    = "ack_policy";
    /** ack_wait */                  String ACK_WAIT                      = "ack_wait";
    /** action */                    String ACTION                        = "action";
    /** active */                    String ACTIVE                        = "active";
    /** allow_atomic */              String ALLOW_ATOMIC                  = "allow_atomic";
    /** allow_direct */              String ALLOW_DIRECT                  = "allow_direct";
    /** allow_msg_schedules */       String ALLOW_MSG_SCHEDULES           = "allow_msg_schedules";
    /** allow_msg_counter */         String ALLOW_MSG_COUNTER             = "allow_msg_counter";
    /** allow_msg_ttl */             String ALLOW_MSG_TTL                 = "allow_msg_ttl";
    /** allow_rollup_hdrs */         String ALLOW_ROLLUP_HDRS             = "allow_rollup_hdrs";
    /** alternates */                String ALTERNATES                    = "alternates";
    /** api */                       String API                           = "api";
    /** api_url */                   String API_URL                       = "api_url";
    /** auth_required */             String AUTH_REQUIRED                 = "auth_required";
    /** average_processing_time */   String AVERAGE_PROCESSING_TIME       = "average_processing_time";
    /** backoff */                   String BACKOFF                       = "backoff";
    /** batch */                     String BATCH                         = "batch";
    /** bucket */                    String BUCKET                        = "bucket";
    /** bytes */                     String BYTES                         = "bytes";
    /** chunks */                    String CHUNKS                        = "chunks";
    /** client_id */                 String CLIENT_ID                     = "client_id";
    /** client_ip */                 String CLIENT_IP                     = "client_ip";
    /** cluster */                   String CLUSTER                       = "cluster";
    /** code */                      String CODE                          = "code";
    /** compression */               String COMPRESSION                   = "compression";
    /** config */                    String CONFIG                        = "config";
    /** connect_urls */              String CONNECT_URLS                  = "connect_urls";
    /** consumer_count */            String CONSUMER_COUNT                = "consumer_count";
    /** consumer_seq */              String CONSUMER_SEQ                  = "consumer_seq";
    /** consumer_limits */           String CONSUMER_LIMITS               = "consumer_limits";
    /** consumers */                 String CONSUMERS                     = "consumers";
    /** count */                     String COUNT                         = "count";
    /** created */                   String CREATED                       = "created";
    /** current */                   String CURRENT                       = "current";
    /** data */                      String DATA                          = "data";
    /** deleted */                   String DELETED                       = "deleted";
    /** deleted_details */           String DELETED_DETAILS               = "deleted_details";
    /** deliver */                   String DELIVER                       = "deliver";
    /** deliver_group */             String DELIVER_GROUP                 = "deliver_group";
    /** deliver_policy */            String DELIVER_POLICY                = "deliver_policy";
    /** deliver_subject */           String DELIVER_SUBJECT               = "deliver_subject";
    /** delivered */                 String DELIVERED                     = "delivered";
    /** deny_delete */               String DENY_DELETE                   = "deny_delete";
    /** deny_purge */                String DENY_PURGE                    = "deny_purge";
    /** description */               String DESCRIPTION                   = "description";
    /** dest */                      String DEST                          = "dest";
    /** digest */                    String DIGEST                        = "digest";
    /** discard */                   String DISCARD                       = "discard";
    /** discard_new_per_subject */   String DISCARD_NEW_PER_SUBJECT       = "discard_new_per_subject";
    /** domain */                    String DOMAIN                        = "domain";
    /** duplicate */                 String DUPLICATE                     = "duplicate";
    /** duplicate_window */          String DUPLICATE_WINDOW              = "duplicate_window";
    /** endpoints */                 String ENDPOINTS                     = "endpoints";
    /** durable_name */              String DURABLE_NAME                  = "durable_name";
    /** err_code */                  String ERR_CODE                      = "err_code";
    /** error */                     String ERROR                         = "error";
    /** errors */                    String ERRORS                        = "errors";
    /** expires */                   String EXPIRES                       = "expires";
    /** expires_in */                String EXPIRES_IN                    = "expires_in";
    /** external */                  String EXTERNAL                      = "external";
    /** filter */                    String FILTER                        = "filter";
    /** filter_subject */            String FILTER_SUBJECT                = "filter_subject";
    /** filter_subjects */           String FILTER_SUBJECTS               = "filter_subjects";
    /** first_seq */                 String FIRST_SEQ                     = "first_seq";
    /** first_ts */                  String FIRST_TS                      = "first_ts";
    /** flow_control */              String FLOW_CONTROL                  = "flow_control";
    /** go */                        String GO                            = "go";
    /** group */                     String GROUP                         = "group";
    /** hdrs */                      String HDRS                          = "hdrs";
    /** headers */                   String HEADERS                       = "headers";
    /** headers_only */              String HEADERS_ONLY                  = "headers_only";
    /** host */                      String HOST                          = "host";
    /** id */                        String ID                            = "id";
    /** idle_heartbeat */            String IDLE_HEARTBEAT                = "idle_heartbeat";
    /** inactive_threshold */        String INACTIVE_THRESHOLD            = "inactive_threshold";
    /** inflight */                  String INFLIGHT                      = "inflight";
    /** internal */                  String INTERNAL                      = "internal";
    /** jetstream */                 String JETSTREAM                     = "jetstream";
    /** keep */                      String KEEP                          = "keep";
    /** lag */                       String LAG                           = "lag";
    /** ldm */                       String LAME_DUCK_MODE                = "ldm";
    /** last_active */               String LAST_ACTIVE                   = "last_active";
    /** last_by_subj */              String LAST_BY_SUBJECT               = "last_by_subj";
    /** last_error */                String LAST_ERROR                    = "last_error";
    /** last_seq */                  String LAST_SEQ                      = "last_seq";
    /** last_ts */                   String LAST_TS                       = "last_ts";
    /** leader */                    String LEADER                        = "leader";
    /** leader_since */              String LEADER_SINCE                  = "leader_since";
    /** level */                     String LEVEL                         = "level";
    /** limit */                     String LIMIT                         = "limit";
    /** limits */                    String LIMITS                        = "limits";
    /** link */                      String LINK                          = "link";
    /** lost */                      String LOST                          = "lost";
    /** max_ack_pending */           String MAX_ACK_PENDING               = "max_ack_pending";
    /** max_age */                   String MAX_AGE                       = "max_age";
    /** max_batch */                 String MAX_BATCH                     = "max_batch";
    /** max_bytes */                 String MAX_BYTES                     = "max_bytes";
    /** max_bytes_required */        String MAX_BYTES_REQUIRED            = "max_bytes_required";
    /** max_consumers */             String MAX_CONSUMERS                 = "max_consumers";
    /** max_chunk_size */            String MAX_CHUNK_SIZE                = "max_chunk_size";
    /** max_deliver */               String MAX_DELIVER                   = "max_deliver";
    /** max_expires */               String MAX_EXPIRES                   = "max_expires";
    /** max_memory */                String MAX_MEMORY                    = "max_memory";
    /** max_msg_size */              String MAX_MSG_SIZE                  = "max_msg_size";
    /** max_msgs */                  String MAX_MSGS                      = "max_msgs";
    /** max_msgs_per_subject */      String MAX_MSGS_PER_SUB              = "max_msgs_per_subject";
    /** max_payload */               String MAX_PAYLOAD                   = "max_payload";
    /** max_storage */               String MAX_STORAGE                   = "max_storage";
    /** max_streams */               String MAX_STREAMS                   = "max_streams";
    /** max_waiting */               String MAX_WAITING                   = "max_waiting"; // this is correct! the meaning name is different than the field name
    /** min_pending */               String MIN_PENDING                   = "min_pending";
    /** min_ack_pending */           String MIN_ACK_PENDING               = "min_ack_pending";
    /** mem_storage */               String MEM_STORAGE                   = "mem_storage";
    /** memory */                    String MEMORY                        = "memory";
    /** memory_max_stream_bytes */   String MEMORY_MAX_STREAM_BYTES       = "memory_max_stream_bytes";
    /** message */                   String MESSAGE                       = "message";
    /** messages */                  String MESSAGES                      = "messages";
    /** metadata */                  String METADATA                      = "metadata";
    /** mtime */                     String MTIME                         = "mtime";
    /** mirror */                    String MIRROR                        = "mirror";
    /** mirror_direct */             String MIRROR_DIRECT                 = "mirror_direct";
    /** msgs */                      String MSGS                          = "msgs";
    /** multi_last */                String MULTI_LAST                    = "multi_last";
    /** name */                      String NAME                          = "name";
    /** next_by_subj */              String NEXT_BY_SUBJECT               = "next_by_subj";
    /** no_ack */                    String NO_ACK                        = "no_ack";
    /** no_erase */                  String NO_ERASE                      = "no_erase";
    /** no_wait */                   String NO_WAIT                       = "no_wait";
    /** nonce */                     String NONCE                         = "nonce";
    /** nuid */                      String NUID                          = "nuid";
    /** num_ack_pending */           String NUM_ACK_PENDING               = "num_ack_pending";
    /** num_deleted */               String NUM_DELETED                   = "num_deleted";
    /** num_errors */                String NUM_ERRORS                    = "num_errors";
    /** num_pending */               String NUM_PENDING                   = "num_pending";
    /** num_redelivered */           String NUM_REDELIVERED               = "num_redelivered";
    /** num_replicas */              String NUM_REPLICAS                  = "num_replicas";
    /** num_requests */              String NUM_REQUESTS                  = "num_requests";
    /** num_subjects */              String NUM_SUBJECTS                  = "num_subjects";
    /** num_waiting */               String NUM_WAITING                   = "num_waiting";
    /** offline */                   String OFFLINE                       = "offline";
    /** offset */                    String OFFSET                        = "offset";
    /** opt_start_seq */             String OPT_START_SEQ                 = "opt_start_seq";
    /** opt_start_time */            String OPT_START_TIME                = "opt_start_time";
    /** options */                   String OPTIONS                       = "options";
    /** paused */                    String PAUSED                        = "paused";
    /** pause_remaining */           String PAUSE_REMAINING               = "pause_remaining";
    /** pause_until */               String PAUSE_UNTIL                   = "pause_until";
    /** persist_mode */              String PERSIST_MODE                  = "persist_mode";
    /** pinned_client_id */          String PINNED_CLIENT_ID              = "pinned_client_id";
    /** pinned_ts */                 String PINNED_TS                     = "pinned_ts";
    /** placement */                 String PLACEMENT                     = "placement";
    /** port */                      String PORT                          = "port";
    /** priority */                  String PRIORITY                      = "priority";
    /** priority_groups */           String PRIORITY_GROUPS               = "priority_groups";
    /** priority_policy */           String PRIORITY_POLICY               = "priority_policy";
    /** priority_timeout */          String PRIORITY_TIMEOUT              = "priority_timeout";
    /** processing_time */           String PROCESSING_TIME               = "processing_time";
    /** proto */                     String PROTO                         = "proto";
    /** purged */                    String PURGED                        = "purged";
    /** push_bound */                String PUSH_BOUND                    = "push_bound";
    /** queue_group */               String QUEUE_GROUP                   = "queue_group";
    /** raft_group */                String RAFT_GROUP                    = "raft_group";
    /** raise_status_warnings */     String RAISE_STATUS_WARNINGS         = "raise_status_warnings";
    /** rate_limit_bps */            String RATE_LIMIT_BPS                = "rate_limit_bps";
    /** replay_policy */             String REPLAY_POLICY                 = "replay_policy";
    /** replica */                   String REPLICA                       = "replica";
    /** replicas */                  String REPLICAS                      = "replicas";
    /** republish */                 String REPUBLISH                     = "republish";
    /** request */                   String REQUEST                       = "request";
    /** reserved_memory */           String RESERVED_MEMORY               = "reserved_memory";
    /** reserved_storage */          String RESERVED_STORAGE              = "reserved_storage";
    /** response */                  String RESPONSE                      = "response";
    /** retention */                 String RETENTION                     = "retention";
    /** sample_freq */               String SAMPLE_FREQ                   = "sample_freq";
    /** schema */                    String SCHEMA                        = "schema";
    /** sealed */                    String SEALED                        = "sealed";
    /** seq */                       String SEQ                           = "seq";
    /** server_id */                 String SERVER_ID                     = "server_id";
    /** server_name */               String SERVER_NAME                   = "server_name";
    /** size */                      String SIZE                          = "size";
    /** source */                    String SOURCE                        = "source";
    /** sources */                   String SOURCES                       = "sources";
    /** src */                       String SRC                           = "src";
    /** started */                   String STARTED                       = "started";
    /** start_time */                String START_TIME                    = "start_time";
    /** state */                     String STATE                         = "state";
    /** stats */                     String STATS                         = "stats";
    /** storage */                   String STORAGE                       = "storage";
    /** storage_max_stream_bytes */  String STORAGE_MAX_STREAM_BYTES      = "storage_max_stream_bytes";
    /** stream_name */               String STREAM_NAME                   = "stream_name";
    /** stream_seq */                String STREAM_SEQ                    = "stream_seq";
    /** stream */                    String STREAM                        = "stream";
    /** streams */                   String STREAMS                       = "streams";
    /** subject */                   String SUBJECT                       = "subject";
    /** subject_delete_marker_ttl */ String SUBJECT_DELETE_MARKER_TTL     = "subject_delete_marker_ttl";
    /** subject_transform */         String SUBJECT_TRANSFORM             = "subject_transform";
    /** subject_transforms */        String SUBJECT_TRANSFORMS            = "subject_transforms";
    /** subjects */                  String SUBJECTS                      = "subjects";
    /** subjects_filter */           String SUBJECTS_FILTER               = "subjects_filter";
    /** success */                   String SUCCESS                       = "success";
    /** system_account */            String SYSTEM_ACCOUNT                = "system_account";
    /** tags */                      String TAGS                          = "tags";
    /** template_owner */            String TEMPLATE_OWNER                = "template_owner";
    /** threshold_percent */         String THRESHOLD_PERCENT             = "threshold_percent";
    /** tiers */                     String TIERS                         = "tiers";
    /** time */                      String TIME                          = "time";
    /** ts */                        String TIMESTAMP                     = "ts";
    /** tls_required */              String TLS_REQUIRED                  = "tls_required";
    /** tls_available */             String TLS_AVAILABLE                 = "tls_available";
    /** total */                     String TOTAL                         = "total";
    /** traffic_account */           String TRAFFIC_ACCOUNT               = "traffic_account";
    /** type */                      String TYPE                          = "type";
    /** up_to_seq */                 String UP_TO_SEQ                     = "up_to_seq";
    /** up_to_time */                String UP_TO_TIME                    = "up_to_time";
    /** val */                       String VAL                           = "val";
    /** version */                   String VERSION                       = "version";

    /**
     * Use TLS_REQUIRED instead
     */
    @Deprecated
    String TLS = "tls_required";
}
