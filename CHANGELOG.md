# Change Log

## 2.27.0

### Core
* sendPing removes its pong future when queueing the PING throws #1636
* SNI and TLS hostname verification support #1638
* Reject options that ask for both the default and the trust-all SSL context #1639
* Hostname verification on by default #1640
* Reject a ws:// server when the options carry an SSL context #1641
* Upgrade bcprov-lts8on to 2.73.13 #1656
* Fix read-only protection for header entry views #1658

### Core Connection ADR-40
* [ADR40] URL credentials: carry to discovered servers, replace nkey, JSON-encode token #1663
* [ADR40] TLS mismatch: close the connection after a full round of mismatches; tlsFirst selects the default context #1665
* [ADR40] Plaintext INFO before TLS: serves the attempt only, wait for the INFO after the first PONG on 2.12, bytes after INFO fail the handshake #1666
* [ADR40] TLS discovery and reconnect #1668

### JS / KV / OS
* Close the dispatcher when a KV or Object Store watch fails to start #1643
* Object store get: no wait for an empty object, no NPE on a single chunk with no data #1644 

### Service / Conformance
* ADR Conformance, Service, C-103 and C-107 #1645
* ADR Conformance, Service, C-415 #1646
* ADR Conformance, Service, C-309 and C-310 #1647
* ADR Conformance, Service, C-404 #1648
* Validate queue names as an ADR-6 queue-name #1649
* ADR Conformance, Service, C-601, C-602 and C-603 #1650
* ADR Conformance, Service, C-203 and C-204 #1651
* Service respondError and ServiceReplyMessage #1652
* Service response builder #1654
* Service respondError overloads in place of the response builder #1655


## 2.26.4

### Core
* Make outgoing pending count getters non-blocking #1632

### JetStream
* Fix consume with batch size 1 stopping after one message #1633

### Test
* Fix max bytes tests to respect the server 2.16 default ack format v2 #1630

### CI/CD
* Branch snapshot versions: plain on main, branch name elsewhere, no / in the version #1634

## 2.26.3

### Core
* Fix pending queue lookup race in message delivery #1615

### JetStream
* Support ADR 60 Ack Policy Flow Control #1624
* Support ADR 50 Fast Ingest with constants #1628

### Service
* Fix discoverMany handling of a no responders status #1620

### Test
* Update Test Server Runner (Major Version) #1617
* Fix test to respect server 2.15 change to max_consumers behavior #1623

### CI/CD
* Update GitHub Actions to Node 24 majors, install nats-server via shared action #1618
* Add a Windows build workflow #1619
* Update org.bouncycastle:bcprov-lts8on  #1622 @bugada

## 2.26.2

### Core
* Update the status before tearing the socket down in closeSocket #1609

### JetStream
* Fix regression of PublishOptions getStreamTimeout #1611 @utamas

### Miscellaneous
* test user pass with special characters fixed to run on all platforms #1605
* Connection Listener Stale State Tests #1607
* Fix Test Flappers #1612

## 2.26.1

### Core
* Reset Reader Connection #1592
* Simplify set reader connection #1594
* Count connect failure once per server, not once per resolved IP #1595 @stasimus
* Address forceReconnectImpl reader/writer stop race #1601

### JetStream
* Better exception raising on create consumer #1598

### Miscellaneous
* Additional tests for reader repoint #1593
* Add provider dependency note to readme #1599
* Add docs.nats.io examples to main #1600
* Instructions for Building a Canary #1602

## 2.26.0

### Core
* Reconnect Delay Behavior and options cleanup #1578
* Advanced Request Behavior Option #1582
* Optional individual thread factories for reader and writer #1583
* Tidy executor options and additional testing #1590

### JetStream
* Handle consume initial subscription failure. #1573
* Miscellaneous Improvements based on V3 work #1577
* Properly default idle heartbeat during setter. #1580
* Consumer Source Field Nullability #1587

### Misc
* Add Client and Orbit section, drop duplicate Orbit link #1572 @Jarema
* Change optional skip claude target #1574
* Update README.md - Fix example links #1584 @github-pawo

### New Nats Docs
* \[New Nats Docs Examples] fixed missing marker #1571
* \[New Nats Docs Examples] Normalize examples host #1575

## 2.25.3

### Core
* More schedule headers to match updated server implementation #1543
* Headers object toString bug #1545
* WebSocket masking used % 8 instead of % 4 — #1546
* NatsConnection fine tuning variable access #1547
* Flush Immediately After Publish Fix #1550
* Finds while building J3 #1554

### JetStream
* Update api objects based on schema review #1549
* ObjectInfo getObjectName() should be marked @NonNull #1559
* 2.14 Stream Configuration #1561
* 2.14 Reset Consumer #1562
* Flow Control $JS.FC support #1563
* 2.14 Stream Consumer Source support #1565
* Reset Consumer Test Flapper #1567
* Reset Consumer correct API response and improve test #1568

### Testing
* Fix test, server 2.12.5 allows update max consumers #1538

### Miscellaneous
* Document use of ConsumerInfo management call #1539
* Add Readme index to Consumer Info Calls #1541
* Move examples to subproject #1544

### CI/CD
* Setup claude to only manually trigger #1540
* Testing claude script v2 #1542
* Fix Claude workflow configuration #1551 @Jarema
* Change claude to always review #1569

### New Nats Docs Examples
* Queue Group Doc Examples were switched #1553
* \[New Nats Docs Examples] Complete Missing Examples #1556
* \[New Nats Docs Examples] SubjectsSingleWildcard #1557
* \[New Nats Docs Examples] Update Single and Multi Wildcard #1558
* \[New Nats Docs Examples] Misc Examples Improvements #1560
* \[New Nats Docs Examples] Improved Comment #1564
* \[New Nats Docs Examples] Fix start marker #1566

## 2.25.2
### Core
* fix race condition in reconnect #1523 @jkraml-staffbase
* "Payload Size" includes header bytes, not just data #1525 @francoisprunier
* NatsConnection improvements: reader and writer replace, options executors use #1526
* HostnameResolveMode better organizes the 4 possible modes #1536 @bartosz822

### JetStream
* Simplification track user processed not client received. #1528
* New Examples for Priority Groups #1529 @roeschter
* StreamInfo fix active to handle -1 and not supplied correctly #1531

### Testing
* SSL Error Raising Testing #1532
* Cleanup temp files and folders in tests #1534

### Miscellaneous
* Updating Badges #1521
* Housekeeping #1535


## 2.25.1
### Core
* Fix setting options via properties #1489
* Lazy initialize Options executors #1492
* Adding flexibility for executor options #1493
* [bug] Properly count message/bytes when in discardWhenFull mode #1498
* Subject validation #1501
* Call to error listener should have been done in a callback, not directly #1502
* Message Queue Splitting #1505
* NatsConnectionReader better ReadListener call #1507
* NatsConnectionReader better exceptions #1509
* Enable path component when connecting via WebSocket #1514 @jbaeck

### Object Store
* Object Store purge on re-put #1491

### Miscellaneous
* Documenting Uber Jar and fixing example class usages. #1496
* Fix Flapper Test testSocketDataPortTimeout #1500
* Subject validation readme and backfill #1503
* Doc examples #1508
* Fix line ends #1511
* Doc examples #1513

## 2.24.1

### Build
* Fix removed Automatic-Module-Name #1486 @derklaro

### Core
* Properly unsubscribe from dispatcher subject #1483
* Internal Options Executor Awareness #1484

## 2.24.0

### Core
* [BUG] Properly return pending bytes #1437
* Improve ConnectionListener #1438
* Server Pool Improvement #1443
* Improved reconnect determining when to switch queues #1444
* Improve output queue push locking and error message #1446
* ConnectionListener event time #1448
* Optimize header reading #1449
* Header fine tuning #1451
* Header tuning don't over engineer #1453
* Micro-optimization getBytes(ISO_8859_1) vs getBytes(US_ASCII) #1455
* Reduce calls to stats by combining. #1465
* [BUG] MessageQueue drainTo was not properly copying length and size. #1468
* MessageQueue drainTo more #1469
* Auth Error is allowed to be set before being connected #1475

### JetStream
* Stream Config Persist mode #1436
* Review methods with varargs for proper validation #1456
* [BUG] AccountTier read reserved values as int instead of long #1457
* Handle both wrong last sequence 10071 / 10164 #1463
* Pinned Consumer Support #1472

### Docs
* Fixed the Typo in the Example Code #1450  @sikandar

### Test
* Refactor test to validate issue 1426 #1447
* Additional test coverage #1452
* Demonstrate Pull Status Warning Handling #1454
* Miscellaneous Test Coverage #1460
* Fix and Enable ReconnectTests testForceReconnectQueueBehaviorCheck #1473
* Javadoc Technical Debt #1476

### Build
* Build modifications for Coveralls #1459

## 2.23.0

### Server 2.12 Specific
* (2.12) All 2.12 PRs combined #1403*
    * (2.12) ClusterInfo field updates #1408
    * (2.12) Support Stream Config allow_msg_schedules #1422
    * (2.12) Message Counter support #1423
    * (2.12) Support Batch Publish #1425
    * (2.12) Prioritized Consumer Support #1433

### Core
* Make NatsStatistics public so it can be extended #1414
* Statistics classes improvements #1415
* Lock around access to pending message and byte counts #1416
* Socket Read Timeout remove validation #1417
* Ensure write timeout is not less than 100 nanoseconds #1429
* Options to set underlying socket configuration of SO_SNDBUF and SO_RCVBUF #1432


### JetStream
* Exposed StreamName #1431 @roeschter

### Misc
* Add getter for ObjectMeta Metadata to ObjectInfo #1418
* Update SocketDataPortBlockSimulator #1430

## 2.22.0

### Core
* Expose connection outgoingPendingMessageCount and outgoingPendingBytes #1412

### JetStream
* Add publish header "Nats-Expected-Last-Subject-Sequence-Subject" #1401
* Add publish header "Nats-Expected-Last-Subject-Sequence-Subject" part b. #1402
* [Fix] Reset cached consumer info when re-making ordered consumers #1405
* consumers - metadata comparison #1410

### ObjectStore
* ObjectMeta metadata #1399

### Misc
* Clarify documentation for JetStreamSubscription #1400 @davidmorley
* Build (CI/CD) Stuff #1404
* Update version of bouncycastle to resolve CVE-2025-9341 #1406 @vkolomeyko

## 2.21.5

### Core
* Implement Fast fallback algorithm in SocketDataPort #1351 @jitinsharma
* [FIX] Shutdown internal executors on connection close. #1357
* Convert serverAuthErrors to ConcurrentHashMap #1359
* Fix SSL handshake listeners never removed, preventing garbage collection #1360
* Fix core unsubscribe by subject #1363
* Move from JetBrains nullability annotations to JSpecify #1366
* Fix timeout computing to address possible nano time overflow #1375 @magicprinc
* Create NatsInetAddress to allow replacement of InetAddress #1378 @jitinsharma
* Json parser tuning. #1382
* Connection and adjacent objects nullability markers. #1383
* Headers +toString(), fixes #1385 @magicprinc
* Header nullability #1395
* Updated SSL Files and Build preparing for server 2.12 #1397

### JetStream
* [Fix] Simplified Ordered Consumer - Getting name early can cause NPE #1354
* Stream Name cannot be null #1377
* Fix JetStreamApiException constructor nullability conflict #1379
* Ensuring nullability contracts #1387

### Key Value
* Add revision guard on KV key "create" #1356
* Fixed limit marker test for server change #1367
* Additional KV Marker / TTL testing #1391

### Testing / Examples / Docs / Etc
* Ordered Consumer Examples #1352
* Update readme for fast fallback option #1353 @jitinsharma
* Improved Consumer Name Testing #1365
* Review for Issues (#1361 and #1362) #1368
* Unit test refactor #1369
* Use String.replace instead of String.replaceAll #1381 @magicprinc
* Fixed Jacoco from considering test classes #1390
* Fixing flapping tests #1393
* Fix test flappers #1396

## 2.21.4

### Core
* Fix race condition during reconnect sends UNSUB messages #1321 @ajax-surovskyi-y
* Add connection auth token supplier option #1324 @buleuszmatak
* Nano time for elapsed timings and Nats System Clock #1334
* Replace Timer with scheduled tasks #1335
* [Bug] Fix UNSUBs after disconnect can cause auth violations #1336 @ajax-surovskyi-y
* Options allow token supplier from property, not just api method #1349

### JetStream
* Annotating API objects with NotNull and Nullable #1333
* [Bug] MessageConsumer.isFinished() not set properly in certain conditions #1339
* [Bug] Pull Heartbeat handler intermittent failure after switch to scheduler #1345
* Fix heartbeat timer handling broken when replacing timer with scheduler. #1348

### Key Value
* Nats-Marker-Reason must be mapped to a Key Value Operation #1323
* KV Purge Per Message TTL #1344

### Tests
* Addition validation and test for token / token supplier #1325
* Add test for auth violations during reconnect #1328 @ajax-surovskyi-y
* Fixed KV Limit Marker Test to only run against 2.11.2 or later #1338
* Fix flapping test: testOverflowFetch #1340
* Set the test timeout default to 3 minutes. #1343

###  Misc
* Better Json Print Formatter #1327

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
* Update account ApiStats with level and inflight #1304
* Update account AccountTier with reserved memory and reserved storage #1305
* Add StreamAlternate structure to StreamInfo response #1306
* Better workflow for leadership change while pull consuming. #1313
* Ability to supply a prefix for watches. Fixed ordered consumer naming bug. #1314
* Use full NUID for generated consumer names #1317

### Key Value
* Update KV consumeKeys() to return the BlockingQueue immediately. #1308
* KV Limit Marker #1310 
* KV LimitMarker add missing getter, additional docs and tests #1311

### General
* Update repository info and cleanup readme. #1318

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
* KV TTL (stream max_age) versus stream duplicate_window #1301

### JetStream
* ConsumeOptions creation from json should use default, not minimum #1302

### Misc
* Remove STAN references #1300

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
* Handle Server 2.10.26 returns No Responders instead of timeouts. #1292

### Jetstream
* Improve FetchConsumeOptions construction and add test #1293

### 2.11 Specific
Main 2 11 merge safe #1294 is actually a compilation of PRs related to 2.11 features  @MauriceVanVeen
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
* Reader Listener #1265
* [BUG] Hosts should never be resolved on websocket URI's #1286
* Replace ed25519 with BouncyCastle #1290

### KV
* KV Minor Changes #1264
* Tuning common code used for watches and key lookup #1281

### Service API
* Remove Schema related leftovers #1263 @d0x7
* Feature: Support adding service endpoint after construction #1274
* Feature: Support adding service endpoint after construction (more) #1276
* Service user endpoints can be run without queue groups. #1277
* Service, tuning review #1279

### Documentation
* Docs - close() versus stop() in MessageConsumer #1271 @roeschter
* Remove the experimental mentions #1278
* Fix Javadoc for JetStream #1283 @vkolomeyko
* [DOC] ResilientPublisher better api doc #1285

### Test
* Add test coverage #1282

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
* Edit lock don't unlock if was not locked. #1255
* Executor and Executor Factories in Options can be created via properties #1257

### JetStream
* Fix simplified ordered consuming when a delivery policy was set. #1251 @roeschter
* Fix: exception on simple consume when disconnected immediately #1253 @roeschter

### Documentation
* NoWait documentation #1256 @roeschter

### Tests
* Unit test coverage #1252

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
* Don't flush after the request from publishAsync calls #1220

### JetStream
* Easier access to timeout in JetStream implementations #1236

### Object Store
* Minor object store improvement - complete faster #1237

### Documentation
* Javadocs Improvements #1221 @roeschter
* Callout external docs and examples #1224

### Tests
* Fixed tests for 2.8.4 regression stream and consumer metadata #1225
* Test base code improvements #1240

### Misc
* Add publish of test code jar to builds. #1230

## 2.20.3

SKIPPED

## 2.20.2

### Core
* Publishing plus immediate flush #1211
* When interrupted call interrupt #1212 @MauriceVanVeen
* Interrupt fine tuning #1214
* Run loops check the interrupted flag #1215
* Tuning futures used for internal tracking #1216
* Fix cleanResponses must handle cleaning up canceled future #1218 @MauriceVanVeen

## 2.20.1

### Core
* Catch errors in Dispatchers and log #1200 @MauriceVanVeen
* During forceReconnect, ensure reader/writer is stopped before continuing #1203 @MauriceVanVeen
* While handleCommunicationIssue, closeSocket should use reconnect impl otherwise tryingToConnect will guard #1206 @MauriceVanVeen 
* Remove redundant writer.flushBuffer() call #1207 @MauriceVanVeen

### Documentation
* Jetstream Usage #1193 @roeschter
* Recommended (new) APIs and more example code #1191 @roeschter

## 2.20.0

### Core
* CONNECT username and password need json encoding #1168
* UTF subject support #1169
* Handle InterruptedException when obtaining the lock to queue a message. #1170
* Handle duration string when accepting duration properties #1183
* Make NatsServerPool extendable #1185
* Allow user to set socket read timeout option #1188

### JetStream
* Stream state subjects as a map #1177
* Stream state more efficient loading of subjects #1179

### Key Value
* Add filtering to KV keys methods #1173
* Add filtering to KV keys methods - Use greater than constant #1174
* Add filtering to KV keys methods - remove unnecessary optimization #1175

### Object Store
* Object Store use over leaf-hub domain part 2 #1172

### Unit Testing
* Test implementation dependency update #1167
* Improve cluster testing support #1171
* Change test to run against 2.10 or later #1180 #1181
* Flapper Reconnect test fix #1189

### Project Management
* Switch to Eclipse Temurin and add SDKMan for local dev #1176 @artur-ciocanu
* Allowing building main on workflow_dispatch #1186

## 2.19.1

### Core
* [Bug] Maximum Message Size accepted a long, but should accept an int #1161
* [Bug] Poison pill comparison was failing #1162
* Augmented force reconnect #1165

### Object Store
* Object Store use over leaf-hub domain #1160

### Misc
* Serialization changed, fixed javadocs #1159

## 2.19.0
### Core
Improve socket close behavior #1155

### Tests
Additional NKey Tests #1154

### Misc
Experimental Retrier utility removed to its own project/repo #1153

## 2.18.1
#### Core
* General Retry Functionality #1145
* Cluster string in Placement is optional. #1150
* Remove duplicate reconnect event #1151

#### JetStream
* Support 409 Leadership Change #1144

#### Doc
* Update docs and comments (MessageQueue, forceReconnect) #1142


## 2.18.0 / 2.17.7

2.18.0 and 2.17.7 are identical.

2.18.0 attempts to start us on the road to properly [Semantic Version (semver)](https://semver.org/). In the last few patch releases, changes that should have been exposed via a minor version bump were numbered as a patch.

Even if just one api is newly added, semver requires that we bump the minor version. The `forceReconnect` api is an example of one api being added to the Connection interface. It should have resulted in a minor version bump.

Going forward, when a release contains only bug fixes, it's appropriate to simply bump the patch. But if an api is added, even one, then the minor version will be bumped.


#### Core
* Revert headers from being read-only upon message creation #1123
* tls_available support #1127
* Revisit Write Timeout Handling #1128
* Remove rethrowable runtime exception during connection #1137 @ajax-surovskyi-y

#### JetStream
* Fix #1125 NatsJetStreamMetaData timestamp timezone #1126 @mrmx

#### Doc
* Update Readme with links to docs and examples. #1119

#### Misc
* Refactor Encoding util for easier replacement of Base64 encoding class #1121



## 2.17.6 / 2.17.5

2.17.6 was released immediately after 2.17.5. The allowing of a comma delimited list of keys was removed.
This is noted since it was technically available for several hours even though it was never publicly announced,
and will be considered a bug fix.

#### Core
* Log connect trace server uri when reconnecting #1106 @photizzo
* Ensure NatsConnection inboxDispatcher is started prior to publishing messages #1109 @nathanschile

#### JetStream
* Add support for Consumer Create Action #1108

#### KV
* KV Watch Multiple Filters #1113
* KV Watch Multiple Filters Additional Test #1114
* KV Watch Multiple Filters remove comma delimited support #1117

### Simplification

#### Test
* Testing Cleanup Only - No Functionality Changes #1110
* Don't run testAddStreamInvalids with server older than 2.10 #1111

#### Doc
* Added graalvm setup example #1112 @YunaBraska
* Graalvm readme touch-up #1115

#### Misc

## 2.17.4

#### Core
* Fix reconnect() calls close() when simultaneously 'disconnecting' in another thread #1082 @MauriceVanVeen
* Optional dispatching with executor instead of blocking #1083
* Offer timeout millis no longer hardcoded #1088
* ReentrantLock instead of synchronized #1095
* Force Reconnect API #1100
* SSLContextFactory #1101

#### JetStream
* Implement Consumers Pause #1093 @MauriceVanVeen
* Make sure Fetch No Wait returns as soon as it gets 404/408 #1096
* Message Info always use last value in the headers. #1099

#### KV
* KV Watch From Revision #1081
* KeyValue atomic delete and purge methods. #1092 @davidmcote
* KV Transform support. #1098

### Simplification
* Allow simplification fetch to have noWait with or without expires. #1089

#### Test
* Add Test for NUID to handle Sequence and Concurrency #1094 @photizzo

#### Doc
* upgrade documentation to specify backoff behavior #1078 @imclem

#### Misc
* Chaos App tuning and additional testing while porting. #1080

## 2.17.3

#### Core
* Socket Data Port with Manual Write Timeout #1064
* Provide implementation instead of empty trust manager check #1066

#### Test
* Reconnect On Connect Unit Test #1062 #1063

#### Doc
* Document Message Immutability Headers Bug #1059

## 2.17.2

Robustness and recovery

#### Core
* Support StreamConfiguration creation from JSON #1023 @senior
* TimeoutException instead of CancellationException on timeout #1031 @maximbreitman
* Message Headers Improvement #1054

#### JetStream
* Pull Request Validate Idle Heartbeat Against Expiration #1036
* Client side consumer recovery #1043
* Ensure robustness b/w compatible with server 2.8.4 #1046
* Chaos Testing Tuning #1055

#### Extensions (KV / OS / Service)
* KV and OS Compression and Metadata #1034

#### Docs / Testing / Examples / Etc.
* Initialize SocketDataPort directly #1022
* Add dockerfile for compatibility tests #1024 @piotrpio
* Update jnats-server-runner to 1.2.6 #1027
* Auth Test Improvement #1028
* Test Base Tuning #1030
* Publish Expectations Test for JetStream API Code #1032
* More Tests for recent NatsRequestCompletableFuture enhancement #1035
* Add nats by example link #1037 @Jarema
* Improve test for real user authentication expired #1040
* Update readme / better subject validation notes #1047
* Fetch Resilient Example #1048
* Refactoring and improving example code. #1049

## 2.17.1

This release added was mostly about TLS and testing

#### Core

* Allow trust store without key store #999 @jnmoyne
* Allow for custom dispatcher #1001
* Expanded support for server sent authorization errors #1006
* Reconnect on Connect additional api #1012
* Accept Tls First as an Option #1015

#### JetStream

* Duplicate message for Interest stream fails seq check in PublishAck #1005 @MauriceVanVeen
* Direct Message Subject Header May Contain Multiple Subjects #1016

#### Service Framework

* Added custom endpoint queue support and fixed InfoResponse to show proper subject. #1010

#### Docs / Testing / Misc

* Removing generic-ness from naming #1000 #1004
* Subscribe options name handling vs 2.8.4 #1011
* Client Compatibility Tests engine #1013
* Regression and Test Improvements #1017 #1018

## 2.17.0

This release added:
* support for server 2.10 features
* some additional JetStream tuning
* StatisticsCollector to add flexibility to statistics tracking
* Better ability to create connection options completely from a properties file
* Doc updates.

#### JetStream 2.10 Specific

* stream/consumer info timestamps, stream config first_seq #962
* Multiple Filter Subjects and Subject validation changes #965
* Stream Configuration Compression Option #976
* Subject Transform Stream Configuration Part 1 #978
* Consumer Limits Stream Configuration #979
* Subject Transforms in Mirror/Info and Source/Info #982
* Multiple Filter Subjects Review #984

#### JetStream

* Fast Bind #959
* Provide a default consumerByteCount implementation #969

#### Core

* Add StatisticsCollector to Options for custom statistics tracking #964 @johnlcox
* Construct Options builder from properties file for user. #971
* Revert removal of client side checks #981

#### Docs / Testing / Misc

* Test coverage and add note about websocket to readme. #956
* Add issue forms #957 @bruth
* fixed text in error message #961
* Simplify issue forms #966 @bruth
* README.md library version more visible #972
* Update README to document connect Options properties #973
* update builds to use better way to install server #975
* Small improvements README.md #977 @aaabramov
* Minor Api doc fixes #983

## 2.16.14

This release takes the experimental tag off both the Simplification API and the Service Framework.
* The simplification api has very minor naming changes and the addition of the Ordered Consumer
* The Service Framework was documented and also had some very minor naming changes.
* Added the ability to create complete secure Options for connection via configuration file instead of using builder methods. Please see the readme.

#### Core

* Minor correction and improved variable naming in NatsConnection #945
* Improving Options with better construction via properties #946
* Minor improvements to README.md #947
* Added unit tests for WebSocket + JWT #939
* Jwt Utils update to support "audience" #949

#### Simplification

* Simplified Ordered Consumer #936
* Simplified Queue Example #940
* Ensure run simplification tests only if 2.9.1 or later #942
* Simplification More Review #948
* Simplification state, better thread safety #952

#### Service

* Service / Simplification Documentation and Release Prep #954

#### JetStream

* fixed rate limit comparison bug #951
* Fix use of JsOptions timeout #953

## 2.16.13

#### Core

* Address secure Websocket changes in server 2.9.19 #931

#### JetStream

* [BUG] Consumer Config fields are 32 bits (ints) not 64 bits (longs) #930

#### Simplification

* Simplification Tuning and Review #927
* Simplification Review, Tuning, Examples, Testing 2 #929

#### Misc / Test

* Better testing checks against server versions #932

## 2.16.12

#### Core

* WS only read payload from network stream #902 @GrafBlutwurst
* Remove client side check against server info max_payload when publishing #922

#### JetStream

* Fixed GetChanges for backoff and metadata #910
* Fine tuning message manger result #921
* Remove validation when adding or updating consumer #923

#### Simplification

* Completion, tuning, address feedback, etc. #917 #919 #926

#### Service Framework

* Remove schema from Service API #916
* Change INFO response to contain endpoint details #918
* Fine tune service start / stop #920
* Service code cleanup #925

#### Misc / Test

* Update dependencies.md #904
* Bump GH Actions versions (Go; node12 deprecation) @philpennock
* Enhance jwt utils #906 #909
* Message: Fix typo in javadoc #911 @nicoruti
* Unit test improvements #915

## 2.16.11

#### Core

* New connection option reportNoResponders #887
* WS Http Request Version Default To 1.1 #891 @aditya-agarwal-groww

#### JetStream

* better pull error message and async tests #888
* better handle pull status message comes from a previous pull #899

#### Misc / Test

* Test Improvements #886
* Test tuning #889
* Server regression issue was fixed, setting test back #890
* test harness - more flexible way to wait for errors/exceptions #894
* flappers - trying more time #895
* Update plugin repository to refer to existing services after Bintray shutdown #897 @cpiotr

## 2.16.10

#### JetStream

* For JS Subs, expose the consumer name since it's already tracked #869
* Part 1 in pull handling improvements #870
* Part 2 in pull handling improvements #871
* Time used fetching should never be negative #878 @liu-jianyang
* Pull-fetch-time-left #879 @liu-jianyang
* Simplification Part 1 #848
* Simplification (Experimental) Part 2 #882
* Simplification pre-experimental-release #884

#### Core

* fix rehost #874
* Options Builder constructor that takes existing Options #875
* StreamConfiguration and ConsumerConfiguration metadata support #877

#### Misc / Test

* JsonValue improvement #868
* consume test for bytes can't run until at least 2.9.1 #883
* added metadata map to service and endpoints #862

## 2.16.9

#### Core

* ServerPool including Hostname resolution #847
* Enable add/remove of multiple ConnectionListeners per Connection #861 @davidmcote

#### JetStream

* pull status handling #819

#### Service  (Experimental)

* use standard connection create inbox for discovery #857

#### Object Store (Experimental)

* use -1 to indicate end of stream, not lack of full buffer #852
* digest resets after getting value, so can't call it more than once. #854

#### Misc / Test

* Update docs and examples #846
* 2.10 allows filter subject that doesn't match stream subjects #849
*
## 2.16.8

#### Core and JetStream

* Fill in Publish / Request Header Gaps #839

#### Core

* Dispatcher with no default handler sometimes has NPE during close if messages are still coming in #841
* allow empty headers #844

#### Test

* Server runner update to 1.2.0 #843

## 2.16.7

#### Core

* Options.builder() fluent #832 @nwseOOO

#### Service

* Service multi endpoints #830
* Service fine tuning #833
* Service subject in responses #834

## 2.16.6

#### Core

* message size calculation improvements #820 @MauriceVanVeen
* consistent validation and version comparison fix #823 @pskiwi

#### Service

* Service Beta #813
* service response types #818

#### Key Value

* kv additional update api #814

#### Etc. Tests / Docs / Examples

* fix inactiveThreshold docs in ConsumerConfiguration #809 @MauriceVanVeen
* verify 810 is not a defect #811
* CI deploy snapshot for a branch #817

## 2.16.5

#### Core

* increase default max (pending) messages, ability to set pending limits in subscribe options #799
* better setup of pending limits #804
* ignore auth required flag from server when building connection string #807 @ColinSullivan1

#### Etc. Tests / Docs / Examples

* Fix test only check mirror direct 2.9.0 and later #794
* Fixes typo in Readme #795 @aaabramov
* Better dev version and autobench naming #798
* Change Temporary File Creation in unit test. #800 @JLLeitschuh
* fixed test - name not necessary, better for regression #803

## 2.16.4

#### Key Value (KV)

* kv mirror support #789

#### JetStream

* stream state lost #792

#### Core

* Implement RTT #784
* get client inet address #791

#### Misc

* Minor fix for README.md Jetstream example #790 @JonasPed

## 2.16.3

#### JetStream
* changed handling of missing offset for paged #782

## 2.16.2

#### Core
* reader HMSG protocol line length size was wrong #774
* Message sending optimization #776 @MauriceVanVeen

#### Misc
* Fix Get Streams Doc #771
* PubWithHeadersBenchmark #772

## 2.16.1
#### Core

* Fix - Ensure resizing of the sendBuffer #746 @MauriceVanVeen
* Enhancement - Additional API to clear last error from Nats server #750 @Ryner51
* Sync NatsMessage.getSizeInBytes() with usage in NatsConnectionWriter #756 @MauriceVanVeen

#### JetStream

* discard_new_per_subject #755
* streams / names with subject filtering, info pagination #759
* ordered consumer config setting changes #762
* Ordered Consumer Heartbeat Handling #766

#### KV or OS

* object store tuning #731
* KV status bytes - Issue #754
* List materialized views #765

#### Examples

* Example to show handling pub acks in a separate thread than the publish. #748

## 2.16.0 Support Server v2.9.0 and Object Store Experimental

This release is a re-release of 2.15.7 with an additional opt-out and the minor version bump.

#### JetStream Options Opt-Out
* Ability to opt-out of using Server v2.9.0 consumer create api #728

#### JetStream / Management
* Get Message Enhancement #696
* No Erase option on message delete #698
* Support v2.9.0 Get Direct Message feature #701 #703
* Support v2.9.0 Consumer Create feature #723 #725

#### Key Value
* Fix bug to return null instead of entry on get of deleted or purged key #700
* Allow direct configuration for KV #718 #724

#### Object Store
* Initial implementation, experimental. #705 #714 #720

#### Core
* Static Memory Auth Handler #702
* Support v2.9.0 stream configuration republish #709

## 2.15.7 Support Server v2.9.0 and Object Store Experimental

#### JetStream / Management
* Get Message Enhancement #696
* No Erase option on message delete #698
* Support v2.9.0 Get Direct Message feature #701 #703
* Support v2.9.0 Consumer Create feature #723 #725

#### Key Value
* Fix bug to return null instead of entry on get of deleted or purged key #700
* Allow direct configuration for KV #718 #724

#### Object Store
* Initial implementation, experimental. #705 #714 #720

#### Core
* Static Memory Auth Handler #702
* Support v2.9.0 stream configuration republish #709

## 2.15.6 new Consumer configuration fields

#### Core
* better request timeout management #693

#### JetStream
* support num_replicas and mem_storage in consumer configuration #689 @goku321

#### Misc
* Change example to have more flexibility on message size #690 

## 2.15.5 re-release 2.15.4 fixes, enhancements and experimental

#### Core
* Accept encoded connection urls #674 
* Only track duplicate responses when advanced tracking is on #659

#### JetStream
* revert ConsumerConfiguration changes where some fields were downgraded #685
* consumer info change to sequence_info from sequence_pair #679
* consumer filter subject is now modifiable #676
* handle updated account stats #668
* Ability to create an External object #661

#### KV
* ability to update bucket config #662

#### Experimental
* expose management getStreamNamesBySubjectFilter #681
* experimental pull reader #683

#### Tests
* Add test for NKey.clear #663 @lacinoire

#### Misc
* remove comments that say durable is required during pull #675
* better push queue example #670
* fix inactive threshold doc #660

## 2.15.3 Writeable Placement

#### JetStream

* Ability for user to create a Placement object [PR #655](https://github.com/nats-io/nats.java/pull/655)

## 2.15.2 JetStream Improvements

#### JetStream

* Pull config changes, ephemeral pull, unit tests [PR #645](https://github.com/nats-io/nats.java/pull/645)
* Server urls connection management [PR #648](https://github.com/nats-io/nats.java/pull/648)
  - Architecture issue 113 Add option to ignore discovered urls
  - ServersToTryProvider provide a way that a user can provide a complete custom implementation to provide the server urls to try on connect / reconnect. Tiered servers could be implemented this way. EXPERIMENTAL feature.
* EXPERIMENTAL PullRequestOptions [PR #649](https://github.com/nats-io/nats.java/pull/649)

## 2.15.1 Remove batch size limitations and add 409 support

#### JetStream

* Remove pull batch size limitation [PR #642](https://github.com/nats-io/nats.java/pull/642)
* Statuses with code 409 are known statuses [PR #643](https://github.com/nats-io/nats.java/pull/643)


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
