/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class LogVerifier {
    @Mock
    private Appender<ILoggingEvent> mockAppender;
    // Captor is genericised with ch.qos.logback.classic.spi.LoggingEvent
    @Captor
    private ArgumentCaptor<LoggingEvent> captorLoggingEvent;

    protected void setup() {
        MockitoAnnotations.initMocks(this);
        final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.addAppender(mockAppender);
    }

    protected void teardown() {
        final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.detachAppender(mockAppender);
    }

    LogVerifier() {

    }

    void verifyLogMsgEquals(Level level, String msg) {
        // Now verify our logging interactions
        List<LoggingEvent> events = captorLoggingEvent.getAllValues();
        if (events.size() > 1) {
            System.err.println("Captured logging events: ");
            for (Iterator<LoggingEvent> it = events.iterator(); it.hasNext();) {
                LoggingEvent ev = it.next();
                System.err.println(ev);
            }
        }
        verify(mockAppender).doAppend(captorLoggingEvent.capture());
        // verify(mockAppender).doAppend(events.get(events.size()));
        // Having a genricised captor means we don't need to cast
        final LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        // Check log level is correct
        assertEquals(level, loggingEvent.getLevel());
        // Check the message being logged is correct
        assertEquals(msg, loggingEvent.getFormattedMessage());
    }

    void verifyLogMsgMatches(Level level, String msg) {
        // Now verify our logging interactions
        List<LoggingEvent> events = captorLoggingEvent.getAllValues();
        if (events.size() > 1) {
            System.err.println("Captured logging events: ");
            for (Iterator<LoggingEvent> it = events.iterator(); it.hasNext();) {
                LoggingEvent ev = it.next();
                System.err.println(ev);
            }
        }
        verify(mockAppender).doAppend(captorLoggingEvent.capture());
        // verify(mockAppender).doAppend(events.get(events.size()));
        // Having a genricised captor means we don't need to cast
        final LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        // Check log level is correct
        assertEquals(level, loggingEvent.getLevel());
        // Check the message being logged is correct
        // assertTrue(loggingEvent.getFormattedMessage().startsWith(msg));
        assertTrue(Pattern.matches(msg, loggingEvent.getFormattedMessage()));
    }

    // @Test
    // public void testSomething() {
    // Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    // final Appender<ILoggingEvent> mockAppender = mock(Appender.class);
    // when(mockAppender.getName()).thenReturn("MOCK");
    // root.addAppender(mockAppender);
    //
    // // ... do whatever you need to trigger the log
    //
    // verify(mockAppender).doAppend((ILoggingEvent) argThat(new ArgumentMatcher() {
    // @Override
    // public boolean matches(final Object argument) {
    // return ((LoggingEvent) argument).getFormattedMessage()
    // .contains("Hey this is the message I want to see");
    // }
    // }));
    // }
    //
    // @Test
    // public void shouldConcatAndLog() {
    // // given
    // ExampleThatLogs example = new ExampleThatLogs();
    // // when
    // final String result = example.concat("foo", "bar");
    // // then
    // assertEquals("foobar", result);
    //
    // // Now verify our logging interactions
    // verify(mockAppender).doAppend(captorLoggingEvent.capture());
    // // Having a genricised captor means we don't need to cast
    // final LoggingEvent loggingEvent = captorLoggingEvent.getValue();
    // // Check log level is correct
    // assertEquals(Level.INFO, loggingEvent.getLevel());
    // // Check the message being logged is correct
    // assertEquals("String a:foo, String b:bar", loggingEvent.getFormattedMessage());
    // }
    //
    // /**
    // * Simple class that we use to trigger a log statement.
    // */
    // public class ExampleThatLogs {
    //
    // private final Logger LOG = (Logger) LoggerFactory.getLogger(ExampleThatLogs.class);
    //
    // public String concat(String a, String b) {
    // LOG.info("String a:" + a + ", String b:" + b);
    // return a + b;
    // }
    // }
}
