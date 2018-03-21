// Copyright 2015-2018 The NATS Authors
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

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.mockito.MockitoAnnotations;

@Category(UnitTest.class)
public class StatisticsTest extends BaseUnitTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
    }

    private Statistics createDummyStats() {
        Statistics stats = new Statistics();
        stats.incrementFlushes();
        stats.incrementInBytes(44L);
        stats.incrementInMsgs();
        stats.incrementOutBytes(199L);
        stats.incrementOutMsgs();
        return stats;
    }

    @Test
    public void testClear() {
        Statistics stats = createDummyStats();
        stats.clear();
        assertEquals(0, stats.getFlushes());
        assertEquals(0, stats.getInBytes());
        assertEquals(0, stats.getInMsgs());
        assertEquals(0, stats.getOutBytes());
        assertEquals(0, stats.getOutMsgs());
        assertEquals(0, stats.getReconnects());
    }

    @Test
    public void testCopyConstructor() {
        Statistics s1 = new Statistics();

        s1.incrementInMsgs();
        s1.incrementInBytes(8192);
        s1.incrementOutMsgs();
        s1.incrementOutBytes(512);
        s1.incrementReconnects();

        Statistics s2 = new Statistics(s1);

        assertEquals(s1.getInMsgs(), s2.getInMsgs());
        assertEquals(s1.getOutMsgs(), s2.getOutMsgs());
        assertEquals(s1.getInBytes(), s2.getInBytes());
        assertEquals(s1.getOutBytes(), s2.getOutBytes());
        assertEquals(s1.getReconnects(), s2.getReconnects());

        assertTrue(s2.equals(s2));
    }

    @Test
    public void testToString() {
        assertNotNull(createDummyStats().toString());
    }

    @Test
    public void testGetInMsgs() {
        Statistics stats = createDummyStats();
        stats.getInMsgs();
    }

    @Test
    public void testIncrementInMsgs() {
        Statistics stats = createDummyStats();
        long n1 = stats.getInMsgs();
        stats.incrementInMsgs();
        assertEquals(n1 + 1, stats.getInMsgs());
    }

    @Test
    public void testGetOutMsgs() {
        Statistics stats = createDummyStats();
        stats.getOutMsgs();
    }

    @Test
    public void testIncrementOutMsgs() {
        Statistics stats = createDummyStats();
        long n1 = stats.getOutMsgs();
        stats.incrementOutMsgs();
        assertEquals(n1 + 1, stats.getOutMsgs());
    }

    @Test
    public void testGetInBytes() {
        Statistics stats = createDummyStats();
        stats.getInBytes();
    }

    @Test
    public void testIncrementInBytes() {
        Statistics stats = createDummyStats();
        long n1 = stats.getInBytes();
        stats.incrementInBytes(1);
        assertEquals(n1 + 1, stats.getInBytes());
    }

    @Test
    public void testGetOutBytes() {
        Statistics stats = createDummyStats();
        stats.getOutBytes();
    }

    @Test
    public void testIncrementOutBytes() {
        Statistics stats = createDummyStats();
        long n1 = stats.getOutBytes();
        stats.incrementOutBytes(1);
        assertEquals(n1 + 1, stats.getOutBytes());
    }

    @Test
    public void testGetReconnects() {
        Statistics stats = createDummyStats();
        stats.getReconnects();
    }

    @Test
    public void testIncrementReconnects() {
        Statistics stats = createDummyStats();
        long n1 = stats.getReconnects();
        stats.incrementReconnects();
        assertEquals(n1 + 1, stats.getReconnects());
    }

    @Test
    public void testGetFlushes() {
        Statistics stats = createDummyStats();
        stats.getFlushes();
    }

    @Test
    public void testIncrementFlushes() {
        Statistics stats = createDummyStats();
        long n1 = stats.getFlushes();
        stats.incrementFlushes();
        assertEquals(n1 + 1, stats.getFlushes());
    }
}
