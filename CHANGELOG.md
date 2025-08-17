# Change Log

## 2.21.5

### Core
* Implement Fast fallback algorithm in SocketDataPort #1351 @jitinsharma
* [FIX] Shutdown internal executors on connection close. #1357 @scottf
* Convert serverAuthErrors to ConcurrentHashMap #1359 @scottf
* Fix SSL handshake listeners never removed, preventing garbage collection #1360 @scottf
* Fix core unsubscribe by subject #1363 @scottf
* Move from JetBrains nullability annotations to JSpecify #1366
* Fix timeout computing to address possible nano time overflow #1375 @scottf @magicprinc
* Create NatsInetAddress to allow replacement of InetAddress #1378 @scottf @jitinsharma
* Json parser tuning. #1382 @scottf
* Connection and adjacent objects nullability markers. #1383 @scottf
* Headers +toString(), fixes #1385 @magicprinc
* Header nullability #1395 @scottf
* Updated SSL Files and Build preparing for server 2.12 #1397 @scottf

### JetStream
* [Fix] Simplified Ordered Consumer - Getting name early can cause NPE #1354 @scottf
* Stream Name cannot be null #1377 @scottf
* Fix JetStreamApiException constructor nullability conflict #1379 @scottf
* Ensuring nullability contracts #1387 @scottf

### Key Value
* Add revision guard on KV key "create" #1356 @scottf
* Fixed limit marker test for server change #1367 @scottf
* Additional KV Marker / TTL testing #1391 @scottf

### Testing / Examples / Docs / Etc
* Ordered Consumer Examples #1352 @scottf
* Update readme for fast fallback option #1353 @jitinsharma
* Improved Consumer Name Testing #1365 @scottf
* Review for Issues (#1361 and #1362) #1368 @scottf
* Unit test refactor #1369
* Use String.replace instead of String.replaceAll #1381 @scottf @magicprinc
* Fixed Jacoco from considering test classes #1390 @scottf
* Fixing flapping tests #1393 @scottf
* Fix test flappers #1396 @scottf

## 2.21.4

### Core
* Fix race condition during reconnect sends UNSUB messages #1321 @ajax-surovskyi-y
* Add connection auth token supplier option #1324 @buleuszmatak
* Nano time for elapsed timings and Nats System Clock #1334 @scottf
* Replace Timer with scheduled tasks #1335 @scottf
* [Bug] Fix UNSUBs after disconnect can cause auth violations #1336 @scottf @ajax-surovskyi-y
* Options allow token supplier from property, not just api method #1349 @scottf

### JetStream
* Annotating API objects with NotNull and Nullable #1333 @scottf
* [Bug] MessageConsumer.isFinished() not set properly in certain conditions #1339 @scottf
* [Bug] Pull Heartbeat handler intermittent failure after switch to scheduler #1345 @scottf
* Fix heartbeat timer handling broken when replacing timer with scheduler. #1348 @scottf

### Key Value
* Nats-Marker-Reason must be mapped to a Key Value Operation #1323 @scottf
* KV Purge Per Message TTL #1344 @scottf

### Tests
* Addition validation and test for token / token supplier #1325 @scottf
* Add test for auth violations during reconnect #1328 @ajax-surovskyi-y
* Fixed KV Limit Marker Test to only run against 2.11.2 or later #1338 @scottf
* Fix flapping test: testOverflowFetch #1340 @scottf
* Set the test timeout default to 3 minutes. #1343 @scottf

###  Misc
* Better Json Print Formatter #1327 @scottf

## 2.21.3

** DO NOT USE **

Use 2.21.4 instead

```
┌─────────────────────┬───────────────────┬─────────────────┬──────────────────────────┬──────────────────┐
│                     │             count │            time │                 msgs/sec │        bytes/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ PubAsync            │  250,000,000 msgs │       136.2:821 │      30,863.438 msgs/sec │      7.36 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubFetch            │  250,000,000 msgs │      174.52:136 │      23,914.410 msgs/sec │      5.70 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubIterate          │  250,000,000 msgs │       100.7:909 │      41,654.780 msgs/sec │      9.93 mb/sec │
└─────────────────────┴───────────────────┴─────────────────┴──────────────────────────┴──────────────────┘
```

## 2.21.2
### Core
* Changed header value validation to accept any ascii except CR & LF #1316 @francoisprunier

### JetStream
* Update account ApiStats with level and inflight #1304 @scottf
* Update account AccountTier with reserved memory and reserved storage #1305 @scottf
* Add StreamAlternate structure to StreamInfo response #1306 @scottf
* Better workflow for leadership change while pull consuming. #1313 @scottf
* Ability to supply a prefix for watches. Fixed ordered consumer naming bug. #1314 @scottf
* Use full NUID for generated consumer names #1317 @scottf

### Key Value
* Update KV consumeKeys() to return the BlockingQueue immediately. #1308 @scottf
* KV Limit Marker #1310  @scottf
* KV LimitMarker add missing getter, additional docs and tests #1311 @scottf

### General
* Update repository info and cleanup readme. #1318 @scottf

```
┌─────────────────────┬───────────────────┬─────────────────┬──────────────────────────┬──────────────────┐
│                     │             count │            time │                 msgs/sec │        bytes/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ PubAsync            │   50,000,000 msgs │       28:02.729 │      29,713.638 msgs/sec │      7.08 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubFetch            │   49,999,991 msgs │       36:03.206 │      23,113.837 msgs/sec │      5.51 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubIterate          │   50,000,000 msgs │       20:02.740 │      41,571.745 msgs/sec │      9.91 mb/sec │
└─────────────────────┴───────────────────┴─────────────────┴──────────────────────────┴──────────────────┘
```

## 2.21.1
### KV
* KV TTL (stream max_age) versus stream duplicate_window #1301 @scottf

### JetStream
* ConsumeOptions creation from json should use default, not minimum #1302

### Misc
* Remove STAN references #1300 @scottf

```
┌─────────────────────┬───────────────────┬─────────────────┬──────────────────────────┬──────────────────┐
│                     │             count │            time │                 msgs/sec │        bytes/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ PubAsync            │   50,000,000 msgs │       28:01.156 │      29,741.440 msgs/sec │      7.09 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubFetch            │   50,000,000 msgs │       38:01.867 │      21,911.882 msgs/sec │      5.22 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubIterate          │   50,000,000 msgs │       20:27.331 │      40,738.806 msgs/sec │      9.71 mb/sec │
└─────────────────────┴───────────────────┴─────────────────┴──────────────────────────┴──────────────────┘
```

## 2.21.0
### Core
* Handle Server 2.10.26 returns No Responders instead of timeouts. #1292 @scottf

### Jetstream
* Improve FetchConsumeOptions construction and add test #1293 @scottf

### 2.11 Specific
Main 2 11 merge safe #1294 is actually a compilation of PRs related to 2.11 features  @scottf @MauriceVanVeen
* Main for server v2.11 #1239
* Consumer Priority Group Overflow #1233
* Add Message TTL Stream Configuration #1280
* Per Message TTL Support for 2.11 #1295

```
┌─────────────────────┬───────────────────┬─────────────────┬──────────────────────────┬──────────────────┐
│                     │             count │            time │                 msgs/sec │        bytes/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ PubAsync            │   50,000,000 msgs │       29:10.069 │      28,570.302 msgs/sec │      6.81 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubFetch            │   49,999,993 msgs │       36:15.122 │      22,987.213 msgs/sec │      5.48 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubIterate          │   50,000,000 msgs │       21:02.180 │      39,614.001 msgs/sec │      9.44 mb/sec │
└─────────────────────┴───────────────────┴─────────────────┴──────────────────────────┴──────────────────┘
```

## 2.20.6
### Core
* Reader Listener #1265 @scottf
* [BUG] Hosts should never be resolved on websocket URI's #1286 @scottf
* Replace ed25519 with BouncyCastle #1290 @scottf

### KV
* KV Minor Changes #1264 @scottf
* Tuning common code used for watches and key lookup #1281 @scottf

### Service API
* Remove Schema related leftovers #1263 @d0x7
* Feature: Support adding service endpoint after construction #1274 @scottf
* Feature: Support adding service endpoint after construction (more) #1276 @scottf
* Service user endpoints can be run without queue groups. #1277 @scottf
* Service, tuning review #1279 @scottf

### Documentation
* Docs - close() versus stop() in MessageConsumer #1271 @roeschter
* Remove the experimental mentions #1278 @scottf
* Fix Javadoc for JetStream #1283 @vkolomeyko
* [DOC] ResilientPublisher better api doc #1285 @scottf

### Test
* Add test coverage #1282 @scottf

### Benchmark

```
┌─────────────────────┬───────────────────┬─────────────────┬──────────────────────────┬──────────────────┐
│                     │             count │            time │                 msgs/sec │        bytes/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ PubAsync            │   50,000,000 msgs │       30:37.925 │      27,204.592 msgs/sec │      6.49 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubFetch            │   50,000,000 msgs │       34:11.735 │      24,369.608 msgs/sec │      5.81 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubIterate          │   50,000,000 msgs │       18:25.347 │      45,234.664 msgs/sec │     10.78 mb/sec │
└─────────────────────┴───────────────────┴─────────────────┴──────────────────────────┴──────────────────┘
```

## 2.20.5

### Core
* feat(nats-connection): implement named executor thread factories #1254 @kedzie
* Edit lock don't unlock if was not locked. #1255 @scottf
* Executor and Executor Factories in Options can be created via properties #1257 @scottf

### JetStream
* Fix simplified ordered consuming when a delivery policy was set. #1251 @scottf @roeschter
* Fix: exception on simple consume when disconnected immediately #1253 @roeschter

### Documentation
* NoWait documentation #1256 @roeschter

### Tests
* Unit test coverage #1252 @scottf

### Benchmark

```
┌─────────────────────┬───────────────────┬─────────────────┬──────────────────────────┬──────────────────┐
│                     │             count │            time │                 msgs/sec │        bytes/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ PubAsync            │   50,000,000 msgs │       28:37.522 │      29,111.709 msgs/sec │      6.94 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubFetch            │   50,000,000 msgs │       35:23.774 │      23,542.995 msgs/sec │      5.61 mb/sec │
├─────────────────────┼───────────────────┼─────────────────┼──────────────────────────┼──────────────────┤
│ SubIterate          │   50,000,000 msgs │       17:10.329 │      48,528.189 msgs/sec │     11.57 mb/sec │
└─────────────────────┴───────────────────┴─────────────────┴──────────────────────────┴──────────────────┘
```

## 2.20.4

### Core
* Don't flush after the request from publishAsync calls #1220 @scottf

### JetStream
* Easier access to timeout in JetStream implementations #1236 @scottf

### Object Store
* Minor object store improvement - complete faster #1237 @scottf

### Documentation
* Javadocs Improvements #1221 @roeschter
* Callout external docs and examples #1224 @scottf

### Tests
* Fixed tests for 2.8.4 regression stream and consumer metadata #1225 @scottf
* Test base code improvements #1240 @scottf

### Misc
* Add publish of test code jar to builds. #1230 @scottf

## 2.20.3

SKIPPED

## 2.20.2

### Core
* Publishing plus immediate flush #1211 @scottf
* When interrupted call interrupt #1212 @MauriceVanVeen
* Interrupt fine tuning #1214 @scottf
* Run loops check the interrupted flag #1215 @scottf
* Tuning futures used for internal tracking #1216 @scottf
* Fix cleanResponses must handle cleaning up cancelled future #1218 @MauriceVanVeen

## 2.20.1

### Core
* Catch errors in Dispatchers and log #1200 @MauriceVanVeen
* During forceReconnect, ensure reader/writer is stopped before continuing #1203 @MauriceVanVeen @scottf
* While handleCommunicationIssue, closeSocket should use reconnect impl otherwise tryingToConnect will guard #1206 @MauriceVanVeen  @scottf
* Remove redundant writer.flushBuffer() call #1207 @MauriceVanVeen

### Documentation
* Jetstream Usage #1193 @roeschter
* Recommended (new) APIs and more example code #1191 @roeschter

## 2.20.0

### Core
* CONNECT username and password need json encoding #1168 @scottf
* UTF subject support #1169 @scottf
* Handle InterruptedException when obtaining the lock to queue a message. #1170 @scottf
* Handle duration string when accepting duration properties #1183 @scottf
* Make NatsServerPool extendable #1185 @scottf
* Allow user to set socket read timeout option #1188 @scottf

### JetStream
* Stream state subjects as a map #1177 @scottf
* Stream state more efficient loading of subjects #1179 @scottf

### Key Value
* Add filtering to KV keys methods #1173 @scottf
* Add filtering to KV keys methods - Use greater than constant #1174 @scottf
* Add filtering to KV keys methods - remove unnecessary optimization #1175 @scottf

### Object Store
* Object Store use over leaf-hub domain part 2 #1172 @scottf

### Unit Testing
* Test implementation dependency update #1167 @scottf
* Improve cluster testing support #1171 @scottf
* Change test to run against 2.10 or later #1180 #1181 @scottf
* Flapper Reconnect test fix #1189 @scottf

### Project Management
* Switch to Eclipse Temurin and add SDKMan for local dev #1176 @artur-ciocanu
* Allowing building main on workflow_dispatch #1186 @scottf

## 2.19.1

### Core
* [Bug] Maximum Message Size accepted a long, but should accept an int #1161 @scottf
* [Bug] Poison pill comparison was failing #1162 @scottf
* Augmented force reconnect #1165 @scottf

### Object Store
* Object Store use over leaf-hub domain #1160 @scottf

### Misc
* Serialization changed, fixed javadocs #1159 @scottf

## 2.19.0
### Core
Improve socket close behavior #1155 @scottf

### Tests
Additional NKey Tests #1154 @scottf

### Misc
Experimental Retrier utility removed to its own project/repo #1153 @scottf

## 2.18.1
#### Core
* General Retry Functionality #1145 @scottf
* Cluster string in Placement is optional. #1150 @scottf
* Remove duplicate reconnect event #1151 @scottf

#### JetStream
* Support 409 Leadership Change #1144 @scottf

#### Doc
* Update docs and comments (MessageQueue, forceReconnect) #1142 @scottf


## 2.18.0 / 2.17.7

2.18.0 and 2.17.7 are identical.

2.18.0 attempts to start us on the road to properly [Semantic Version (semver)](https://semver.org/). In the last few patch releases, changes that should have been exposed via a minor version bump were numbered as a patch.

Even if just one api is newly added, semver requires that we bump the minor version. The `forceReconnect` api is an example of one api being added to the Connection interface. It should have resulted in a minor version bump.

Going forward, when a release contains only bug fixes, it's appropriate to simply bump the patch. But if an api is added, even one, then the minor version will be bumped.


#### Core
* Revert headers from being read-only upon message creation #1123 @scottf
* tls_available support #1127 @scottf
* Revisit Write Timeout Handling #1128 @scottf
* Remove rethrowable runtime exception during connection #1137 @ajax-surovskyi-y

#### JetStream
* Fix #1125 NatsJetStreamMetaData timestamp timezone #1126 @mrmx

#### Doc
* Update Readme with links to docs and examples. #1119 @scottf

#### Misc
* Refactor Encoding util for easier replacement of Base64 encoding class #1121 @scottf



## 2.17.6 / 2.17.5

2.17.6 was released immediately after 2.17.5. The allowing of a comma delimited list of keys was removed.
This is noted since it was technically available for several hours even though it was never publicly announced,
and will be considered a bug fix.

#### Core
* Log connect trace server uri when reconnecting #1106 @photizzo
* Ensure NatsConnection inboxDispatcher is started prior to publishing messages #1109 @nathanschile

#### JetStream
* Add support for Consumer Create Action #1108 @scottf

#### KV
* KV Watch Multiple Filters #1113 @scottf
* KV Watch Multiple Filters Additional Test #1114 @scottf
* KV Watch Multiple Filters remove comma delimited support #1117

### Simplification

#### Test
* Testing Cleanup Only - No Functionality Changes #1110 @scottf
* Don't run testAddStreamInvalids with server older than 2.10 #1111 @scottf

#### Doc
* Added graalvm setup example #1112 @YunaBraska
* Graalvm readme touch-up #1115 @scottf

#### Misc

## 2.17.4

#### Core
* Fix reconnect() calls close() when simultaneously 'disconnecting' in another thread #1082 @MauriceVanVeen
* Optional dispatching with executor instead of blocking #1083 @scottf
* Offer timeout millis no longer hardcoded #1088 @scottf
* ReentrantLock instead of synchronized #1095 @scottf
* Force Reconnect API #1100 @scottf
* SSLContextFactory #1101 @scottf

#### JetStream
* Implement Consumers Pause #1093 @MauriceVanVeen
* Make sure Fetch No Wait returns as soon as it gets 404/408 #1096 @scottf
* Message Info always use last value in the headers. #1099 @scottf

#### KV
* KV Watch From Revision #1081 @scottf
* KeyValue atomic delete and purge methods. #1092 @davidmcote
* KV Transform support. #1098 @scottf

### Simplification
* Allow simplification fetch to have noWait with or without expires. #1089 @scottf

#### Test
* Add Test for NUID to handle Sequence and Concurrency #1094 @photizzo

#### Doc
* upgrade documentation to specify backoff behavior #1078 @imclem

#### Misc
* Chaos App tuning and additional testing while porting. #1080 @scottf

## 2.17.3

#### Core
* Socket Data Port with Manual Write Timeout #1064 @scottf
* Provide implementation instead of empty trust manager check #1066 @scottf

#### Test
* Reconnect On Connect Unit Test #1062 #1063 @scottf

#### Doc
* Document Message Immutability Headers Bug #1059 @scottf

## 2.17.2

Robustness and recovery

#### Core
* Support StreamConfiguration creation from JSON #1023 @senior
* TimeoutException instead of CancellationException on timeout #1031 @maximbreitman
* Message Headers Improvement #1054 @scottf

#### JetStream
* Pull Request Validate Idle Heartbeat Against Expiration #1036 @scottf
* Client side consumer recovery #1043 @scottf
* Ensure robustness b/w compatible with server 2.8.4 #1046 @scottf
* Chaos Testing Tuning #1055 @scottf

#### Extensions (KV / OS / Service)
* KV and OS Compression and Metadata #1034 @scottf

#### Docs / Testing / Examples / Etc.
* Initialize SocketDataPort directly #1022 @scottf
* Add dockerfile for compatibility tests #1024 @piotrpio
* Update jnats-server-runner to 1.2.6 #1027 @scottf
* Auth Test Improvement #1028 @scottf
* Test Base Tuning #1030 @scottf
* Publish Expectations Test for JetStream API Code #1032 @scottf
* More Tests for recent NatsRequestCompletableFuture enhancement #1035 @scottf
* Add nats by example link #1037 @Jarema
* Improve test for real user authentication expired #1040 @scottf
* Update readme / better subject validation notes #1047 @scottf
* Fetch Resilient Example #1048 @scottf
* Refactoring and improving example code. #1049 @scottf

## 2.17.1

This release added was mostly about TLS and testing

#### Core

* Allow trust store without key store #999 @scottf @jnmoyne
* Allow for custom dispatcher #1001 @scottf
* Expanded support for server sent authorization errors #1006 @scottf
* Reconnect on Connect additional api #1012 @scottf
* Accept Tls First as an Option #1015 @scottf

#### JetStream

* Duplicate message for Interest stream fails seq check in PublishAck #1005 @MauriceVanVeen
* Direct Message Subject Header May Contain Multiple Subjects #1016 @scottf

#### Service Framework

* Added custom endpoint queue support and fixed InfoResponse to show proper subject. #1010 @scottf

#### Docs / Testing / Misc

* Removing generic-ness from naming #1000 #1004 @scottf
* Subscribe options name handling vs 2.8.4 #1011 @scottf
* Client Compatibility Tests engine #1013 @scottf
* Regression and Test Improvements #1017 #1018 @scottf

## 2.17.0

This release added:
* support for server 2.10 features
* some additional JetStream tuning
* StatisticsCollector to add flexibility to statistics tracking
* Better ability to create connection options completely from a properties file
* Doc updates.

#### JetStream 2.10 Specific

* stream/consumer info timestamps, stream config first_seq #962 @scottf
* Multiple Filter Subjects and Subject validation changes #965 @scottf
* Stream Configuration Compression Option #976 @scottf
* Subject Transform Stream Configuration Part 1 #978 @scottf
* Consumer Limits Stream Configuration #979 @scottf
* Subject Transforms in Mirror/Info and Source/Info #982 @scottf
* Multiple Filter Subjects Review #984 @scottf

#### JetStream

* Fast Bind #959 @scottf
* Provide a default consumerByteCount implementation #969 @scottf

#### Core

* Add StatisticsCollector to Options for custom statistics tracking #964 @johnlcox
* Construct Options builder from properties file for user. #971 @scottf
* Revert removal of client side checks #981 @scottf

#### Docs / Testing / Misc

* Test coverage and add note about websocket to readme. #956 @scottf
* Add issue forms #957 @bruth
* fixed text in error message #961 @scottf
* Simplify issue forms #966 @bruth
* README.md library version more visible #972 @scottf
* Update README to document connect Options properties #973 @scottf
* update builds to use better way to install server #975 @scottf
* Small improvements README.md #977 @aaabramov
* Minor Api doc fixes #983 @scottf

## 2.16.14

This release takes the experimental tag off both the Simplification API and the Service Framework.
* The simplification api has very minor naming changes and the addition of the Ordered Consumer
* The Service Framework was documented and also had some very minor naming changes.
* Added the ability to create complete secure Options for connection via configuration file instead of using builder methods. Please see the readme.

#### Core

* Minor correction and improved variable naming in NatsConnection #945 @scottf
* Improving Options with better construction via properties #946 @scottf
* Minor improvements to README.md #947 @scottf
* Added unit tests for WebSocket + JWT #939 @scottf
* Jwt Utils update to support "audience" #949 @scottf

#### Simplification

* Simplified Ordered Consumer #936 @scottf
* Simplified Queue Example #940 @scottf
* Ensure run simplification tests only if 2.9.1 or later #942 @scottf
* Simplification More Review #948 @scottf
* Simplification state, better thread safety #952 @scottf

#### Service

* Service / Simplification Documentation and Release Prep #954 @scottf

#### JetStream

* fixed rate limit comparison bug #951 @scottf
* Fix use of JsOptions timeout #953 @scottf

## 2.16.13

#### Core

* Address secure Websocket changes in server 2.9.19 #931 @scottf

#### JetStream

* [BUG] Consumer Config fields are 32 bits (ints) not 64 bits (longs) #930 @scottf

#### Simplification

* Simplification Tuning and Review #927 @scottf
* Simplification Review, Tuning, Examples, Testing 2 #929 @scottf

#### Misc / Test

* Better testing checks against server versions #932 @scottf

## 2.16.12

#### Core

* WS only read payload from network stream #902 @GrafBlutwurst
* Remove client side check against server info max_payload when publishing #922 @scottf

#### JetStream

* Fixed GetChanges for backoff and metadata #910 @scottf
* Fine tuning message manger result #921 @scottf
* Remove validation when adding or updating consumer #923 @scottf

#### Simplification

* Completion, tuning, address feedback, etc. #917 #919 #926 @scottf

#### Service Framework

* Remove schema from Service API #916 @scottf
* Change INFO response to contain endpoint details #918 @scottf
* Fine tune service start / stop #920 @scottf
* Service code cleanup #925 @scottf

#### Misc / Test

* Update dependencies.md #904 @scottf
* Bump GH Actions versions (Go; node12 deprecation) @philpennock
* Enhance jwt utils #906 #909 @scottf
* Message: Fix typo in javadoc #911 @nicoruti
* Unit test improvements #915 @scottf

## 2.16.11

#### Core

* New connection option reportNoResponders #887 @scottf
* WS Http Request Version Default To 1.1 #891 @aditya-agarwal-groww

#### JetStream

* better pull error message and async tests #888 @scottf
* better handle pull status message comes from a previous pull #899 @scottf

#### Misc / Test

* Test Improvements #886 @scottf
* Test tuning #889 @scottf
* Server regression issue was fixed, setting test back #890 @scottf
* test harness - more flexible way to wait for errors/exceptions #894 @scottf
* flappers - trying more time #895 @scottf
* Update plugin repository to refer to existing services after Bintray shutdown #897 @cpiotr

## 2.16.10

#### JetStream

* For JS Subs, expose the consumer name since it's already tracked #869 @scottf
* Part 1 in pull handling improvements #870 @scottf
* Part 2 in pull handling improvements #871 @scottf
* Time used fetching should never be negative #878 @liu-jianyang
* Pull-fetch-time-left #879 @liu-jianyang
* Simplification Part 1 #848 @scottf
* Simplification (Experimental) Part 2 #882 @scottf
* Simplification pre-experimental-release #884 @scottf

#### Core

* fix rehost #874 @scottf
* Options Builder constructor that takes existing Options #875 @scottf
* StreamConfiguration and ConsumerConfiguration metadata support #877 @scottf

#### Misc / Test

* JsonValue improvement #868 @scottf
* consume test for bytes can't run until at least 2.9.1 #883 @scottf
* added metadata map to service and endpoints #862 @scottf

## 2.16.9

#### Core

* ServerPool including Hostname resolution #847 @scottf
* Enable add/remove of multiple ConnectionListeners per Connection #861 @davidmcote

#### JetStream

* pull status handling #819 @scottf

#### Service  (Experimental)

* use standard connection create inbox for discovery #857 @scottf

#### Object Store (Experimental)

* use -1 to indicate end of stream, not lack of full buffer #852 @scottf
* digest resets after getting value, so can't call it more than once. #854 @scottf

#### Misc / Test

* Update docs and examples #846 @scottf
* 2.10 allows filter subject that doesn't match stream subjects #849 @scottf
*
## 2.16.8

#### Core and JetStream

* Fill in Publish / Request Header Gaps #839 @scottf

#### Core

* Dispatcher with no default handler sometimes has NPE during close if messages are still coming in #841 @scottf
* allow empty headers #844 @scottf

#### Test

* Server runner update to 1.2.0 #843 @scottf

## 2.16.7

#### Core

* Options.builder() fluent #832 @nwseOOO

#### Service

* Service multi endpoints #830 @scottf
* Service fine tuning #833 @scottf
* Service subject in responses #834 @scottf

## 2.16.6

#### Core

* message size calculation improvements #820 @scottf @MauriceVanVeen
* consistent validation and version comparison fix #823 @scottf @pskiwi

#### Service

* Service Beta #813 @scottf
* service response types #818 @scottf

#### Key Value

* kv additional update api #814 @scottf

#### Etc. Tests / Docs / Examples

* fix inactiveThreshold docs in ConsumerConfiguration #809 @MauriceVanVeen
* verify 810 is not a defect #811 @scottf
* CI deploy snapshot for a branch #817 @scottf

## 2.16.5

#### Core

* increase default max (pending) messages, ability to set pending limits in subscribe options #799 @scottf
* better setup of pending limits #804 @scottf
* ignore auth required flag from server when building connection string #807 @scottf @ColinSullivan1

#### Etc. Tests / Docs / Examples

* Fix test only check mirror direct 2.9.0 and later #794 @scottf
* Fixes typo in Readme #795 @aaabramov
* Better dev version and autobench naming #798 @scottf
* Change Temporary File Creation in unit test. #800 @JLLeitschuh
* fixed test - name not necessary, better for regression #803 @scottf

## 2.16.4

#### Key Value (KV)

* kv mirror support #789 @scottf

#### JetStream

* stream state lost #792 @scottf

#### Core

* Implement RTT #784 @scottf
* get client inet address #791 @scottf

#### Misc

* Minor fix for README.md Jetstream example #790 @JonasPed

## 2.16.3

#### JetStream
* changed handling of missing offset for paged #782 @scottf

## 2.16.2

#### Core
* reader HMSG protocol line length size was wrong #774 @scottf
* Message sending optimization #776 @scottf @MauriceVanVeen

#### Misc
* Fix Get Streams Doc #771 @scottf
* PubWithHeadersBenchmark #772 @scottf

## 2.16.1
#### Core

* Fix - Ensure resizing of the sendBuffer #746 @MauriceVanVeen
* Enhancement - Additional API to clear last error from Nats server #750 @Ryner51
* Sync NatsMessage.getSizeInBytes() with usage in NatsConnectionWriter #756 @MauriceVanVeen

#### JetStream

* discard_new_per_subject #755 @scottf
* streams / names with subject filtering, info pagination #759 @scottf
* ordered consumer config setting changes #762 @scottf
* Ordered Consumer Heartbeat Handling #766 @scottf

#### KV or OS

* object store tuning #731 @scottf
* KV status bytes - Issue #754 @scottf
* List materialized views #765 @scottf

#### Examples

* Example to show handling pub acks in a separate thread than the publish. #748 @scottf

## 2.16.0 Support Server v2.9.0 and Object Store Experimental

This release is a re-release of 2.15.7 with an additional opt-out and the minor version bump.

#### JetStream Options Opt-Out
* Ability to opt-out of using Server v2.9.0 consumer create api #728 @scottf

#### JetStream / Management
* Get Message Enhancement #696 @scottf
* No Erase option on message delete #698 @scottf
* Support v2.9.0 Get Direct Message feature #701 #703 @scottf
* Support v2.9.0 Consumer Create feature #723 #725 @scottf

#### Key Value
* Fix bug to return null instead of entry on get of deleted or purged key #700 @scottf
* Allow direct configuration for KV #718 #724 @scottf

#### Object Store
* Initial implementation, experimental. #705 #714 #720 @scottf

#### Core
* Static Memory Auth Handler #702 @scottf
* Support v2.9.0 stream configuration republish #709 @scottf

## 2.15.7 Support Server v2.9.0 and Object Store Experimental

#### JetStream / Management
* Get Message Enhancement #696 @scottf
* No Erase option on message delete #698 @scottf
* Support v2.9.0 Get Direct Message feature #701 #703 @scottf
* Support v2.9.0 Consumer Create feature #723 #725 @scottf

#### Key Value
* Fix bug to return null instead of entry on get of deleted or purged key #700 @scottf
* Allow direct configuration for KV #718 #724 @scottf

#### Object Store
* Initial implementation, experimental. #705 #714 #720 @scottf

#### Core
* Static Memory Auth Handler #702 @scottf
* Support v2.9.0 stream configuration republish #709 @scottf

## 2.15.6 new Consumer configuration fields

#### Core
* better request timeout management #693 @scottf

#### JetStream
* support num_replicas and mem_storage in consumer configuration #689 @goku321 @scottf

#### Misc
* Change example to have more flexibility on message size #690  @scottf

## 2.15.5 re-release 2.15.4 fixes, enhancements and experimental

#### Core
* Accept encoded connection urls #674  @scottf
* Only track duplicate responses when advanced tracking is on #659 @scottf

#### JetStream
* revert ConsumerConfiguration changes where some fields were downgraded #685 @scottf
* consumer info change to sequence_info from sequence_pair #679 @scottf
* consumer filter subject is now modifiable #676 @scottf
* handle updated account stats #668 @scottf
* Ability to create an External object #661 @scottf

#### KV
* ability to update bucket config #662 @scottf

#### Experimental
* expose management getStreamNamesBySubjectFilter #681 @scottf
* experimental pull reader #683 @scottf

#### Tests
* Add test for NKey.clear #663 @lacinoire

#### Misc
* remove comments that say durable is required during pull #675 @scottf
* better push queue example #670 @scottf
* fix inactive threshold doc #660 @scottf

## 2.15.3 Writeable Placement

#### JetStream

* Ability for user to create a Placement object [PR #655](https://github.com/nats-io/nats.java/pull/655) @scottf

## 2.15.2 JetStream Improvements

#### JetStream

* Pull config changes, ephemeral pull, unit tests [PR #645](https://github.com/nats-io/nats.java/pull/645) @scottf
* Server urls connection management [PR #648](https://github.com/nats-io/nats.java/pull/648) @scottf
  - Architecture issue 113 Add option to ignore discovered urls
  - ServersToTryProvider provide a way that a user can provide a complete custom implementation to provide the server urls to try on connect / reconnect. Tiered servers could be implemented this way. EXPERIMENTAL feature.
* EXPERIMENTAL PullRequestOptions [PR #649](https://github.com/nats-io/nats.java/pull/649) @scottf

## 2.15.1 Remove batch size limitations and add 409 support

#### JetStream

* Remove pull batch size limitation [PR #642](https://github.com/nats-io/nats.java/pull/642) @scottf
* Statuses with code 409 are known statuses [PR #643](https://github.com/nats-io/nats.java/pull/643) @scottf


## 2.15.0  Subscription must be made before consumer is created

The order of creating a subscription on the server and creating a consumer on the server matters. Once the consumer is created, there is interest and the server tries to deliver. But if the subscription is not created, the messages are delivered to...nowhere, but are considered delivered.

This was not strictly a problem but it was a race - if the subscription was ready before the consumer was sent messages, then things went fine. Unit test didn't fail. But when we were testing against NGS and in clusters with mixes of JetStream and non-Jetstream servers, the consumer was always ready because of simple latency.

So now the server subscription is always made first avoiding the problem altogether.

See PR #639

## 2.14.2 Consumer Configuration Change Validation

#### Improvements

PR #637
* Added additional validation (unit testing) in relation to PR #635 Improve subscription creation with existing durable to be smarter when comparing provided configuration with server configuration.
* Added more information to the exception message text  by including a list of fields that caused the issue.

## 2.14.1 Improvements, client parity, docs, etc.

#### Client Parity
PR #630 Support for server Consumer feature Backoff Lists.

#### Improvements
Issue #616 / PR #617 Support for timeout propagation in async requests
PR #630 Surfaced delay for nak requests
PR #631 Tune kv subscribe supported functions keys / history / purge
PR #634 Added client side limit checks option to allow turning off client side checks which forces check to server. Default behavior is the same.
PR #635 Improve subscription creation with existing durable to be smarter when comparing provided configuration with server configuration.

#### Bug Fixes
Issue #621 / PR #622 Fixed kv key with dot as part of the key

#### Documentation etc.

PR #612 Version change and miscellaneous documentation.
PR #629 Rate Limit is bytes per second (bps) not messages per second.

## 2.14.0 KV Release

#### Key Value

* KV API Release

#### JetStream

* Allow null or empty subject when appropriate while subscribing / binding
* new JetStreamManagement api `StreamInfo getStreamInfo(String streamName, StreamInfoOptions options)`
* support Stream Configuration and Stream State to reflect server changes up to server V2.7.3
* support Consumer Configuration reflect server changes up to server V2.7.3
* Fixed bug with pull subscribe fetch and iterate where it could wait twice the expiration time and improved implementation to reflect server changes in pull behavior.
* Added combo pull nowait + expires primitive api to match server pull changes.

#### Miscellaneous

* Addressed Info level Cure53 audit item regarding version string.
* Moved JsMultiTool out of example to the [example repo](https://github.com/nats-io/java-nats-examples/tree/main/js-multi-tool).
* Added NatsJsPushSubAsyncQueueDurable example program.
* Unit test improvements to prevent flappers


## 2.13.2 KV Experimental

#### JetStream

KV Experimental

## 2.13.1 Subscription Consumer Configuration Fix

#### JetStream

- This release fixes a bug found in the new subscription enhancements where the comparison of default configuration failed to validate properly against an existing (durable) consumer configuration.

- There are also minor enhancements to the JsMulti tool

## 2.13.0 Subscription Enhancements

#### JetStream

- Subscription validation. See [Subscription Creation](https://github.com/nats-io/nats.java#subscription-creation)
- Flow Control and Heartbeat handling
- Domain Support
- Stream/Subject Binding


## 2.12.0 Server Queue Improvements

This release is the first release to support v2.4.0 of the NATS server. The change covers how queueing is supported in JetStream using the Deliver Group subscribe option.

## 2.11.6 KV beta last release compatible with Server v2.3.4 and older

1. Key Value (KV) Beta: This release includes a beta version of the Key Value functionality. There were multiple PR's involved in KV including new interfaces and new api / protocol enhancements designed to support KV
2. Support for API error code allowing server generated errors to be identified by number instead of text.
3. Stream and Consumer descriptions
4. Publish expectation last subject sequence
5. Advanced stream purge functionality
6. Primitive pull functionality marked as "advanced"

## Version 2.11.5

#### Pull Requests

[ENHANCEMENT] PR #506 handle unsigned long values in JetStream api data.

## Version 2.11.4

Revert performance fix due to high cpu during idle.

## Version 2.11.3

#### Pull Requests

- [ENHANCEMENT] PR #472 / #477 benchmark improvements (@scottf)
- [ENHANCEMENT] PR #473 performance improvements(@scottf)
- [FIXED] PR #475 fixed filter subject (@scottf)
- [EXAMPLES] PR #478 Clarify examples with deliver subjects (@scottf)

## Version 2.11.2

#### Pull Requests
- [ENHANCEMENT] PR #470 JsMultiTool and AutoBench Reporting Enhancements (@scottf)
- [ENHANCEMENT] PR #468 duplicates and orphans (@scottf)
- [FEATURE] PR #467 / #471 Heartbeat and Flow Control (@scottf)
- [FIXED] PR #466 JsMultiTool Queue Fix (@scottf)

## Version 2.11.1

JetStream Multi Tool enhancements and documentation

## Version 2.11.0

#### Issue Bug Fixes
- [FIXED] Issue #340 No connection possible when using multiple servers PR #455 (@scottf)

#### Pull Requests
- [FIXED] PR #451 Header status improvements (@scottf)
- [ENHANCEMENT] PR #452 handle no ack publishing (@scottf)
- [ENHANCEMENT] PR #456 switched to jnats-server-runner library (@scottf)
- [ENHANCEMENT] PR #446 improve cleanup of async responses (@scottf)

#### Issues General Closed
- [NON ISSUE] Issue #298 NatsConnection does not report SSL error (@scottf)
- [WILL NOT IMPLEMENT] Issue #272 Add ability to publish byte arrays with specified offset and length (@scottf)
- [DOCUMENTED] Issue #316 Failure creating a subscription on a (fairly) new connection (@scottf)
- [NON ISSUE] Issue #344 Performance issue when publishing to certain topics (@scottf)
- [WILL NOT IMPLEMENT] Issue #373 Why not netty for networking? (@sasbury)
- [PRE-RELEASE FEATURE REMOVED] Issue #388 In the jetstream subscriber examples... (@scottf)
- [DOCUMENTED] Issue #402 Unable to connect to NATS server via Android Studio
- [DOCUMENTED] Issue #445 NatsConnection.request(Message) does not use Message.replyTo (@scottf)
- [DOCUMENTED] Issue #423 createContext() not documented (@scottf)

## Version 2.10.0

1. JetStream (message and management) support added.
1. Miscellaneous bug fixes.
1. Examples and benchmarks updated
1. Improved unit tests with reusable scaffolding
1. General Improvements

#### Non JetStream Pull Requests
- [GENERAL] PR #358 Use OS Default SecureRandom (@scottf)
- [BUILD] Issue #360 Automatic-Module-Name clause added to jar manifest. (@bjorndarri)
- [BUILD] PR #365 gradle minor improvements, support windows (@scottf)
- [TEST] PR #375 fix test failing because of timeout that aren't testing timing (@scottf)
- [GENERAL] PR #380 Add a flushBuffer API (@ColinSullivan1)
- [GENERAL] PR #383 nuid speed improvements (@scottf)
- [GENERAL] PR #391 reconnect-jitter-handler-serverinfo-tests (@scottf)

#### Issue Features
- [JETSTREAM] Issue #335 Add Message Headers (@scottf)
- [GENERAL] Issue #336 Support no-responders (@scottf)
- [JETSTREAM] Issue #353 Jetstream APIS (@ColinSullivan1) (@scottf)
- [BUILD] Issue #355 Automatic module name (@bjorndarri)
- [GENERAL] Issue #377  Add a flushBuffer Connection API (@ColinSullivan1) Added in PR #380
- [JETSTREAM] Issue #393 Create Asynchronous Jetstream.Publish API (@scottf) Added in PR #398
- [JETSTREAM] Issue #396 Jetstream Consumer Delete API (@scottf) Added in PR #408
- [JETSTREAM] Issue #412 Add a JS management API to get consumer info (@scottf) Added in PR #413

#### Issue Bug Fixes
- [FIXED] Issue #424 ERROR: Subject remapping requires Options.oldRequestStyle()... (@scottf)
- [FIXED] Issue #345 "unable to stop reader thread" log message (@ColinSullivan1) Fixed in PR #427
- [FIXED] Issue #310 NatsConnection.close unnecessarily sleeps for one second (@scottf)

#### Issues General
- [COMMENTED] Issue #341 Why is a char[] more secure then a String for connection auth details? (@scottf)
- [OTHER] Issue #384 Validations on expectedLastSeqence, expectedStream and expectedLastMsgId are not working for jetstream producer (fixed by nats-server PR #1787)

## Version 2.8.0

- [ADDED] #323 Nats.connect v2 credentials (@olicuzo)
- [CHANGED]  #320 Update MAINTAINERS.md (@gcolliso)
- [FIXED] #318 Printing trace when NUID initialization takes long (@matthiashanel)
- [FIXED] #327 Subject Remapping Fix (@brimworks)
- [FIXED] #331 Close connection when flush fails during drain (@matthiashanel)
- [FIXED] #330 Reconnect wait was not being honored (@ColinSullivan1)

## Version 2.6.8

* [FIXED] - #309 - Removed some debug printf statements
* [CHANGED] - Allow disable reconnect buffer by using size of zero
* [CHANGED] - Added option to set the max unsent size at the writer and fail or discard messages
* [CHANGED] - Updated build.gradle to not fail if TRAVIS_BRANCH isn't set

## Version 2.6.6

* [FIXED] - #274 - Added a check to prevent double event notification
* [CHANGED] - #276 - Updated TLS certs to match go client
* [FIXED] - #275 - Updated connect to randomize with the same code as reconnect

## Version 2.6.2, 2.6.3, 2.6.4 & 2.6.5

* [FIXED] - problem with jars being built with jdk 11
* [ADDED] - Automated deploy

## Version 2.6.1

* [FIXED] - #263 - Added server URLs to connect exception (not to auth exception)
* [FIXED] - #262 - Added @deprecated as needed
* [FIXED] - #261 - Added a static credentials implementation that uses char arrays
* [FIXED] - #260 - Moved to nats-server from gnatsd for testing
* [FIXED/CHANGED] - #259 - Double authentication errors from a server during reconnect attempts will result in the connection being closed.
* [FIXED] - #257 - Added connection method to messages that come from subscriptions, dispatchers and requests
* [FIXED] - #243 - Added check for whitespace in subjects and queue names
* [FIXED] - Improved a couple flaky tests

## Version 2.6.0

* [FIXED] - cleaned up use of "chain" instead of "creds"
* [FIXED] - #255 - added special ioexception when possible to indicate an authentication problem on connect
* [FIXED] - #252 - deprecated strings for username/pass/token and use char arrays instead, required changing some other code to CharBuffer
* [ADDED] - Openjdk11 to travis build and updated gradle wrapper to 5.5
* [ADDED] - an option to trace connect timing, including a test and example
* [FIXED/ADDED] - #197 - the ability to use a single dispatcher for multiple message handlers, including multiple subscriptions on a single subject

## Version 2.5.2

* [FIXED] - #244 - fixed an issue with parsing ipv6 addresses in the info JSON, added unit test for parser
* [FIXED] - #245 - fixed a timing bug in nats bench, now subscribers start timing at the first receive
* [FIXED/CHANGED] - #246 - fixed a confusing output from nats bench in CSV mode, now the test count and the total count are printed
* [ADDED] - spring cache to git ignore file
* [ADDED] - support for running nats bench with conscrypt

## Version 2.5.1

* [FIXED] - #239 - cleaned up extra code after SSL connect failure
* [FIXED] - #240 - removed stack trace that was left from debugging
* [FIXED] - #241 - changed method used to create an ssl socket to allow support for conscrypt/wildfly native libraries
* [ADDED] - conscrypt flag to natsautobench to allow testing with native library, requires Jar in class path

## Version 2.5.0

* [CHANGED] added back pressure to message queue on publish, this may effect behavior of multi-threaded publishers so moving minor version

## Version 2.4.5 & 2.4.6

* Clean up for rename to nats.java

## Version 2.4.4

* [FIXED] - #230 - removed extra executor allocation
* [FIXED] - #231 - found a problem with message ordering when filtering pings on reconnect, caused issues with reconnect in general
* [FIXED] - #226 - added more doc about ping intervals and max ping
* [FIXED] - #224 - resolved a latency problem with windows due to the cost of the message queues spinwait/lock
* [CHANGED] - started support for renaming gnatsd to nats-server, full release isn't done so using gnatsd for tests still

## Version 2.4.3

* [FIXED] - #223 - made SID public in the message
* [FIXED] - #227 - changed default thread to be non-daemon and normal priority, fixes shutdown issues
* [FIXED] - minor issue in javadoc that showed up when building on windows
* [ADDED] - test for fast pings and disconnect, duration.zero on nextMsg
* [CHANGED] - accepted pull request to replace explicit thread creation with executor

## Version 2.4.2

* [FIXED] - #217 - added check to "ignore" exceptions from reader during drain
* [CHANGED] - no longer call exception handler for "ignored" communication exceptions during close/drain after close occurs
* [ADDED] - #209 - support for comma separated urls in connect() or server()
* [FIXED] - #206 - incorrect error message when reconnect buffer is overrun
* [CHANGED] - #214 - moved to an executor instead of 1-off threads
* [FIXED] - #220 - an icky timing issue that could delay messages
* [CHANGED] - added larger TCP defaults to improve network performance
* [ADDED] - public method to create an inbox subject using the prefix from options
* [FIXED] - #203 & #204 - Fixed a thread/timing issue with writer and cleaning pong queues

## Version 2.4.1

* [FIXED] - #199 - turns out we had to hard code the manifest to remove the private package

## Version 2.4.0

* [ADDED] - support for JWT-based authentication and NGS
* [FIXED] - issue with norandomize server connect order, it was broken at some point
* [FIXED] #199 - import of a private package
* [FIXED] #195 - issue with "discovered" servers not having a protocol but we tried to parse as uri
* [FIXED] #186, #191 - sneaky issue with connection reader not reseting state on reconnect
* [ADDED] #192 - option to set inbox prefix, default remains the same
* [FIXED] #196 - updated all the versions i could find, and added note in gradle file about them

## Version 2.3.0

* [BREAKING CHANGE] Replaced use of strings for seeds and public keys in NKey code to use char[] to allow better memory security.

## Version 2.2.0

* [ADDED] Support for NKeys, and nonce based auth

## Version 2.1.2

* [FIXED] #181 - issue with default pending limits on consumers not matching doc
* [FIXED] #179 - added version variable for jars in build.gradle to make it easier to change
* [ADDED] Support for UTF8 subjects

## Version 2.1.1

* [FIXED] Issue with version in Nats.java, also updated deploying.md with checklist
* [FIXED] Fixed issue during reconnect where buffered messages blocked protocol messages

## Version 2.1.0

* [ADDED] Support for consumer or connection drain. (New API lead to version bump.)
* [FIXED] Fixed an issue with null pointer when ping/pong and reconnect interacted poorly.

## Version 2.0.2

* [FIXED] In a cluster situation the library wasn't using each server's auth info if it was in the URI.

## Version 2.0.1

* [CHANGED] Request now returns a CompletableFuture to allow more application async options
* [ADDED] Added back OSGI manifest information
* [ADDED] getLastError method to connection
* [ADDED/CHANGED] Implemented noEcho tests, and require the server to support noEcho if it is set

## Version 2.0.0

* [BREAKING CHANGE] Moved build to gradle
* [BREAKING CHANGE] Ground up rewrite to simplify and address API issues, API has changed
* [BREAKING CHANGE] Simplified connection API
* [CHANGED] Removed external dependencies

## Version 1.1-SNAPSHOT

_2017-02-09_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/1.0...HEAD)

* [FIXED] Flush wait interval (the amount of time the flusher waits before checking the flush queue) is once again set at 1ms.
* [FIXED] Do not shuffle entire pool when adding URL from INFO
* [ADDED] Connection name can now be accessed using `Connection#getName()`
* [CHANGED] CI tests now run against both Oracle JDK 8 and OpenJDK 8

## Version 1.0

_2017-02-02_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.7.3...1.0)

* [ADDED] `Nats.connect()` and variants have been added as a preferred method for connecting to NATS. `ConnectionFactory.createConnection()` will also continue to be supported.
* [ADDED] `Connection#getServers()` and `Connection#getDiscoveredServers()` APIs to match Go client
* [ADDED] `isTlsRequired()` and `isAuthRequired()` to match Go client capabilities.
* [CHANGED] Methods that previously threw `TimeoutException` now simply return `null` (for non-`void` methods) or throw `IOException` if their timeout elapses before they complete.
  * `ConnectionFactory#createConnection()`
  * `ConnectionImpl#flush(int timeout)`
  * `SyncSubscription#nextMessage(int timeout)` - returns `null` if the timeout elapses before a message is available.
* [CHANGED] Several constant definitions have been moved to the `Nats` class.

## Version 0.7.3

_2016-11-01_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.7.1...0.7.3)

* [FIXED #83](/../../issues/#83) All thread pool executors are now properly shutdown by `Connection#close()`.

## Version 0.7.1

_2016-10-30_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.7.0...0.7.1)

* [NEW API] `Connection#publish(String subject, String reply, byte[] data, boolean flush)` allows the caller to specify whether a flush of the connection's OutputStream should be forced. The default behavior for the other variants of publish is `false`. This was added to optimize performance for request-reply (used heavily in `java-nats-streaming`). The internal flush strategy of `ConnectionImpl` minimizes flush frequency by using a synchronized flusher thread to 'occasionally' flush. This benefits asynchronous publishing performance, but penalizes request-reply scenarios where a single message is published and then we wait for a reply.
* `NatsBench` can now be configured via properties file (see `src/test/resources/natsbench.properties`)

## Version 0.7.0

_2016-10-19_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.6.0...0.7.0)

* [BREAKING CHANGE] `SyncSubscription#nextMessage()` and its variants now throw `InterruptedException` if the underlying `poll`/`take` operation is interrupted.
* Fixed interrupt handling.
* Removed `Channel` implementation in favor of directly using `BlockingQueue`.

## Version 0.6.0

_2016-10-11_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/0.5.3...0.6.0)

* Implemented asynchronous handling of `INFO` messages, allowing the client to process `INFO` messages that may be received from the server after a connection is already established. These asynchronous `INFO` messages may update the client's list of servers in the connected cluster.
* Added proper JSON parsing via [google/gson](https://github.com/google/gson).
* Cleaned up some threading oddities in `ConnectionImpl`
* Moved async subscription threading mechanics into the `Connection`, similar to the Go client.
* Fixed a number of inconsistencies in how subscription pending limits were handled.
* Removed subscription pending limits from `ConnectionFactory`. These should be set on the Subscription using `Subscription#setPendingLimits()`.
* `ConnectionImpl` is now `public`, to avoid some issues with reflection in Java and reflective languages such as Clojure. Described further in [#35](/../../pull/35) (special thanks to [@mccraigmccraig](https://github.com/mccraigmccraig)).
* [#58](/../../issues/#58) Updated `NUID` implementation to match [the Go version](nats-io/nuid)
* [#48](/../../issues/#48) Fixed an NPE issue in `TCPConnectionMock` when calling `bounce()` (affects tests only).
* [#26](/../../issues/#26) Fixed a problem with `AsyncSubscription` feeder thread not exiting correctly in all cases.
* Updated integration tests to more closely reflect similar Go tests.
* Miscellaneous typo, style and other minor fixes.

## Version 0.5.3

_2016-08-29_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.5.2...jnats-0.5.3)

* Moved `nats_checkstyle.xml` out of `src` tree to avoid jar/bundle filtering

## Version 0.5.2

_2016-08-29_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.5.1...jnats-0.5.2)

* Depends on stable nats-parent-1.1.pom
* Excludes nats_checkstyle.xml from jar/bundle
* Downloads `gnatsd` binary for current arch/os to `target/` for test phase
* Housekeeping changes to travis-ci configuration

## Version 0.5.1

_2016-08-21_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.5.0...jnats-0.5.1)

* Fixed a problem with gnatsd 0.9.2 `connect_urls` breaking the client connect sequence. This field is now ignored.
* Retooled the way that releases are shipped from Travis CI, using the `deploy:` clause and new scripts

## Version 0.3.2

_2016-08-20_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.3.1...jnats-0.3.2)

* Fixed a problem parsing Long from String on Android.

## Version 0.5.0

_2016-08-10_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.4.1...jnats-0.5.0)

* Reverted to Java 1.7 compatibility to avoid Android incompatibility
* Fixed an issue that was preventing TLS connections from reconnecting automatically.
* Fixed an issue with asynchronous subscriptions not terminating their feeder thread.
* Exposed pending limits APIs
* Updated examples to match Go client and added benchmark program
* Integrated [NATS parent POM](https://github.com/nats-io/nats-parent-pom)
* Integrated check style
* Integrated maven-bundle-plugin to provide OSGI compliant java-nats bundle
* Miscellaneous minor bug fixes and javadoc updates

## Version 0.4.1

_2016-04-03  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.4.0...jnats-0.4.1)

* Removed a stray log trace statement in publish that was affecting performance.

## Version 0.4.0

_2016-03-29_  [GitHub Diff](https://github.com/nats-io/java-nats/compare/jnats-0.3.1...jnats-0.4.0)

* Built on JDK 8
* Added NUID (a java implementation of [http://github.com/nats-io/nuid](http://github.com/nats-io/nuid)), an entropy-friendly UUID generator that operates ~40 ns per op
* Connection#newInbox() now uses NUID to generate the unique portion of the inbox name
* Added support for pending byte/msg limits for subscriptions:
* Subscription#setPendingLimits(int msgs, int bytes)
* Made the size of the Connection reconnect (pending) buffer configurable with ConnectionFactory setters and getters
* Optimized parser performance
* Optimized parser handling of large message payloads
* ConnectionFactory will now construct a default URL by combining supplied host, port, user, and password if no URL is directly supplied.
* Fixed a couple of issues with misnamed properties
* Miscellaneous doc corrections/updates

## Version 0.3.1

_2016-01-18_ _Initial public release of java-nats, now available on Maven Central._

* Added support for TLS v1.2
* Numerous performance improvements
* The DisconnectedEventHandler, ReconnectedEventHandler and ClosedEventHandler classes from the Alpha release have been renamed to DisconnectedCallback, ReconnectedCallback and ClosedCallback, respectively.
* Travis CI integration
* Coveralls.io support
* Increased test coverage
