# Pull Behavior

### Legend

| Item | Description |
| --------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Plain Pull:           | {"batch":42}                                                                                                                             |
| No Wait Pull:         | {"batch":42,"no\_wait":true}                                                                                                             |
| Expire Pull:          | {"batch":42,"expires":"2021-02-18T22:41:16.192000000Z"}                                                                                  |
| Expire Past Pull:     | {"batch":42,"expires":"2021-02-18T22:41:16.192000000Z"}, but date is before "now"                                                        |
| Ack Regular:          | +ACK                                                                                                                                     |
| Next Regular (Leave): | +NXT                                                                                                                                     |
| Next Advance:         | +NTX {"batch":1,"expires":"2021-02-18T22:41:16.192000000Z"}                                                                              |
| Pub:                  | Items published before 1st pull/read , Items published before 2nd optional re-pull / 2nd read                                            |
| RePull:               | Whether a second pull was issued between the 1st and 2nd read                                                                            |
| Msgs 1:               | Records read in the 1st read loop, reading all messages including status messages.                                                       |
| Msgs 2:               | Records read in the 2nd read loop, reading all messages including status messages.                                                       |
| Ax vs Bx              | Messages published before the 1st read are given data payload of Ax, messages published before the 2nd read are given data payload of Bx |

### Run Pseudo Code

| Item
| ------------------------------- |
| 1\. Publish Ax (if more than 0) |
| 2\. Pull                        |
| 3\. Sleep (if requested)        |
| 4\. Read Loop                   |
| 5\. Publish Bx                  |
| 6\. Optional Re-Pull            |
| 7\. Sleep (if requested)        |
| 8\. Read Loop                   |

### Behaviors

#### Plain + Next

* Read is unbounded
* Never have to re-issue the pull.
* Ack: +NXT
* Expire: n/a

| Pub | RePull | Msgs 1                  | Msgs 2            |
| --- | ------ | ----------------------- | ----------------- |
| 4,4 | Yes    | A1 A2 A3 A4             | B1 B2 B3 B4       |
| 4,4 | No     | A1 A2 A3 A4             | B1 B2 B3 B4       |
| 8,4 | Yes    | A1 A2 A3 A4 A5 A6 A7 A8 | B1 B2 B3 B4       |
| 8,4 | No     | A1 A2 A3 A4 A5 A6 A7 A8 | B1 B2 B3 B4       |
| 6,4 | Yes    | A1 A2 A3 A4 A5 A6       | B1 B2 B3 B4       |
| 6,4 | No     | A1 A2 A3 A4 A5 A6       | B1 B2 B3 B4       |
| 2,4 | Yes    | A1 A2                   | B1 B2 B3 B4       |
| 2,4 | No     | A1 A2                   | B1 B2 B3 B4       |
| 2,6 | Yes    | A1 A2                   | B1 B2 B3 B4 B5 B6 |
| 2,6 | No     | A1 A2                   | B1 B2 B3 B4 B5 B6 |
| 0,4 | Yes    | No Msgs                 | B1 B2 B3 B4       |
| 0,4 | No     | No Msgs                 | B1 B2 B3 B4       |

#### Plain + Ack

* Read is bound by the batch size.
* Pull must be re-issued once the batch size is satisfied.
* If the server runs out of messages, the batch is still active until the batch is satisfied.
* Ack: +ACK
* Expire: n/a

#### No Wait + Next

* Ack: +NXT
* Expire: n/a

#### No Wait + Ack
* Ack: +ACK
* Expire: n/a

#### Expire + Next + Advance

* Ack: +NXT
* Expire: n/a

#### Expire + Next + Advance + Sleep

* Ack: +NXT
* Expire: n/a


#### Expire + Next + Leave

* Ack: +NXT
* Expire: Leave

#### Expire + Next + Leave + Sleep

* Ack: +NXT
* Expire: Leave

#### Expire + Ack
* Ack: +ACK
* Expire: n/a

#### Expire + Ack + Sleep
* Ack: +ACK
* Expire: n/a

#### Expire + Past + Next + Advance

* Ack: +NXT
* Expire: Advance

#### Expire + Past + Next + Leave

* Ack: +NXT
* Expire: Leave

#### Expire + Past + Ack

* Ack: +ACK
* Expire: n/a
