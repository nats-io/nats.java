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

package io.nats.client.api;

import io.nats.client.support.JsonValue;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonValueUtils.*;

public class StreamState {
    private final long msgs;
    private final long bytes;
    private final long firstSeq;
    private final long lastSeq;
    private final long consumerCount;
    private final long subjectCount;
    private final long deletedCount;
    private final ZonedDateTime firstTime;
    private final ZonedDateTime lastTime;
    private final List<Subject> subjects;
    private final List<Long> deletedStreamSequences;
    private final LostStreamData lostStreamData;
    private final Map<String, Long> subjectMap;

    StreamState(JsonValue vStreamState) {
        msgs = readLong(vStreamState, MESSAGES, 0);
        bytes = readLong(vStreamState, BYTES, 0);
        firstSeq = readLong(vStreamState, FIRST_SEQ, 0);
        lastSeq = readLong(vStreamState, LAST_SEQ, 0);
        consumerCount = readLong(vStreamState, CONSUMER_COUNT, 0);
        firstTime = readDate(vStreamState, FIRST_TS);
        lastTime = readDate(vStreamState, LAST_TS);
        subjectCount = readLong(vStreamState, NUM_SUBJECTS, 0);
        deletedCount = readLong(vStreamState, NUM_DELETED, 0);
        deletedStreamSequences = readLongList(vStreamState, DELETED);
        lostStreamData = LostStreamData.optionalInstance(readValue(vStreamState, LOST));

        subjects = new ArrayList<>();
        subjectMap = new HashMap<>();
        JsonValue vSubjects = readValue(vStreamState, SUBJECTS);
        if (vSubjects != null && vSubjects.map != null) {
            for (String subject : vSubjects.map.keySet()) {
                Long count = getLong(vSubjects.map.get(subject));
                if (count != null) {
                    subjects.add(new Subject(subject, count));
                    subjectMap.put(subject, count);
                }
            }
        }
    }

    /**
     * Gets the message count of the stream.
     *
     * @return the message count
     */
    public long getMsgCount() {
        return msgs;
    }

    /**
     * Gets the byte count of the stream.
     *
     * @return the byte count
     */
    public long getByteCount() {
        return bytes;
    }

    /**
     * Gets the first sequence number of the stream. May be 0 if there are no messages.
     * @return a sequence number
     */
    public long getFirstSequence() {
        return firstSeq;
    }

    /**
     * Gets the time stamp of the first message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getFirstTime() {
        return firstTime;
    }

    /**
     * Gets the last sequence of a message in the stream
     *
     * @return a sequence number
     */
    public long getLastSequence() {
        return lastSeq;
    }

    /**
     * Gets the time stamp of the last message in the stream
     *
     * @return the first time
     */
    public ZonedDateTime getLastTime() {
        return lastTime;
    }

    /**
     * Gets the number of consumers attached to the stream.
     *
     * @return the consumer count
     */
    public long getConsumerCount() {
        return consumerCount;
    }

    /**
     * Gets the count of subjects in the stream.
     *
     * @return the subject count
     */
    public long getSubjectCount() {
        return subjectCount;
    }

    /**
     * Get a list of the Subject objects. May be empty, for instance
     * if the Stream Info request did not ask for subjects or if there are no subjects.
     * @return the list of subjects
     */
    public List<Subject> getSubjects() {
        return subjects;
    }

    /**
     * Get a map of subjects instead of a list of Subject objects.
     * @return the map
     */
    public Map<String, Long> getSubjectMap() {
        return subjectMap;
    }

    /**
     * Gets the count of deleted messages
     *
     * @return the deleted count
     */
    public long getDeletedCount() {
        return deletedCount;
    }

    /**
     * Get a list of the Deleted objects. May be null if the Stream Info request did not ask for subjects
     * or if there are no subjects.
     * @return the list of subjects
     */
    public List<Long> getDeleted() {
        return deletedStreamSequences;
    }

    /**
     * Get the lost stream data information if available.
     * @return the LostStreamData
     */
    public LostStreamData getLostStreamData() {
        return lostStreamData;
    }

    @Override
    public String toString() {
        return "StreamState{" +
            "msgs=" + msgs +
            ", bytes=" + bytes +
            ", firstSeq=" + firstSeq +
            ", lastSeq=" + lastSeq +
            ", consumerCount=" + consumerCount +
            ", firstTime=" + firstTime +
            ", lastTime=" + lastTime +
            ", subjectCount=" + subjectCount +
            ", subjects=" + subjects +
            ", deletedCount=" + deletedCount +
            ", deleteds=" + deletedStreamSequences +
            ", lostStreamData=" + lostStreamData +
            '}';
    }
}
