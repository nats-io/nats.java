/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;

import java.util.ArrayList;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.verification.VerificationMode;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

class LogVerifier {
    @Mock
    private Appender<ILoggingEvent> mockAppender;
    // Captor is genericised with ch.qos.logback.classic.spi.LoggingEvent
    @Captor
    private ArgumentCaptor<LoggingEvent> captorLoggingEvent;

    void setup() {
        MockitoAnnotations.initMocks(this);
        final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.addAppender(mockAppender);
    }

    void teardown() {
        final Logger logger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        logger.detachAppender(mockAppender);
    }

    LogVerifier() {

    }

    void verifyLogMsgEquals(Level level, String msg) {
        verifyLogMsgEquals(level, msg, times(1));
    }

    void verifyLogMsgEquals(Level level, String msg, VerificationMode mode) {
        // Now verify our logging interactions
        verify(mockAppender, mode).doAppend(captorLoggingEvent.capture());
        if (mode != times(1)) {
            List<LoggingEvent> events = captorLoggingEvent.getAllValues();
            List<LoggingEvent> matches = new ArrayList<>();
//            System.err.println("Captured logging events: ");
            for (LoggingEvent ev : events) {
//                System.err.println(ev);
                if (ev.getMessage().equals(msg) && ev.getLevel().equals(level)) {
                    matches.add(ev);
                }
            }
            if (matches.isEmpty()) {
                fail(String.format("No matching event(s) found for Level=%s, Message='%s'", level, msg));
            } else {
                return;
            }
        }

        // Having a genricised captor means we don't need to cast
        final LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        // Check log level is correct
        assertEquals(level, loggingEvent.getLevel());
        // Check the message being logged is correct
        assertEquals(msg, loggingEvent.getFormattedMessage());
    }

    void verifyLogMsgMatches(Level level, String msg) {
        // Now verify our logging interactions
        verify(mockAppender).doAppend(captorLoggingEvent.capture());
        List<LoggingEvent> events = captorLoggingEvent.getAllValues();
        if (events.size() > 1) {
            System.err.println("Captured logging events: ");
            for (LoggingEvent ev : events) {
                System.err.println(ev);
            }
        }
        // verify(mockAppender).doAppend(events.get(events.size()));
        // Having a genricised captor means we don't need to cast
        final LoggingEvent loggingEvent = captorLoggingEvent.getValue();
        // Check log level is correct
        assertEquals(level, loggingEvent.getLevel());
        // Check the message being logged is correct
        // assertTrue(loggingEvent.getFormattedMessage().startsWith(msg));
        assertTrue(Pattern.matches(msg, loggingEvent.getFormattedMessage()));
    }
}
