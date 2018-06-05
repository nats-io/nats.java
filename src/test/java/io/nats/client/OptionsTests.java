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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Properties;

import javax.net.ssl.SSLContext;

import org.junit.Test;

import io.nats.client.ConnectionHandler.Events;
import io.nats.client.ErrorHandler.Errors;

public class OptionsTests {
    @Test
    public void testDefaultOptions() {
        Options o = new Options.Builder().build();
        
        assertEquals("default one server", 1,  o.getServers().size());
        assertEquals("default url", Options.DEFAULT_URL,  o.getServers().toArray()[0].toString());

        assertEquals("default verbose", false, o.isVerbose());
        assertEquals("default pedantic", false, o.isPedantic());
        assertEquals("default tlsDebug", false, o.isTlsDebug());
        assertEquals("default norandomize", false, o.isNoRandomize());
        assertEquals("default oldstyle", false, o.isOldRequestStyle());

        assertNull("default username", o.getUsername());
        assertNull("default password", o.getPassword());
        assertNull("default token", o.getToken());
        assertNull("default connection name", o.getConnectionName());

        assertNull("default ssl context", o.getSslContext());

        assertEquals("default max reconnect", Options.DEFAULT_MAX_RECONNECT, o.getMaxReconnect());
        assertEquals("default ping max", Options.DEFAULT_MAX_PINGS_OUT, o.getMaxPingsOut());
        assertEquals("default reconnect buffer size", Options.DEFAULT_RECONNECT_BUF_SIZE, o.getReconnectBufferSize());

        assertEquals("default reconnect wait", Options.DEFAULT_RECONNECT_WAIT, o.getReconnectWait());
        assertEquals("default connection timeout", Options.DEFAULT_TIMEOUT, o.getConnectionTimeout());
        assertEquals("default ping interval", Options.DEFAULT_PING_INTERVAL, o.getPingInterval());

        assertNull("error handler", o.getErrorHandler());

        assertNull("disconnect handler", o.getDisconnectHandler());
        assertNull("reconnect handler", o.getReconnectHandler());
        assertNull("close handler", o.getCloseHandler());
    }

    @Test
    public void testChainedBooleanOptions() throws NoSuchAlgorithmException {
        Options o = new Options.Builder().verbose().pedantic().tlsDebug().noRandomize().oldRequestStyle().build();
        assertNull("default username", o.getUsername());
        assertEquals("chained verbose", true, o.isVerbose());
        assertEquals("chained pedantic", true, o.isPedantic());
        assertEquals("chained tlsDebug", true, o.isTlsDebug());
        assertEquals("chained norandomize", true, o.isNoRandomize());
        assertEquals("chained oldstyle", true, o.isOldRequestStyle());
    }
    
    @Test
    public void testChainedStringOptions() throws NoSuchAlgorithmException {
        Options o = new Options.Builder().userInfo("hello", "world").token("token").connectionName("name").build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained username", "hello", o.getUsername());
        assertEquals("chained password", "world", o.getPassword());
        assertEquals("chained token", "token", o.getToken());
        assertEquals("chained connection name", "name", o.getConnectionName());
    }

    @Test
    public void testChainedSSLOptions() throws NoSuchAlgorithmException {
        SSLContext ctx = SSLContext.getDefault();
        Options o = new Options.Builder().sslContext(ctx).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained context", ctx, o.getSslContext());
    }

    @Test
    public void testChainedIntOptions() {
        Options o = new Options.Builder().maxReconnects(100).maxPingsOut(200).reconnectBufferSize(300).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained max reconnect", 100, o.getMaxReconnect());
        assertEquals("chained ping max", 200, o.getMaxPingsOut());
        assertEquals("chained reconnect buffer size", 300, o.getReconnectBufferSize());
    }

    @Test
    public void testChainedDurationOptions() {
        Options o = new Options.Builder()
                            .reconnectWait(Duration.ofMillis(101))
                            .connectionTimeout(Duration.ofMillis(202))
                            .pingInterval(Duration.ofMillis(303))
                            .build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained reconnect wait", Duration.ofMillis(101), o.getReconnectWait());
        assertEquals("chained connection timeout", Duration.ofMillis(202), o.getConnectionTimeout());
        assertEquals("chained ping interval", Duration.ofMillis(303), o.getPingInterval());
    }

    @Test
    public void testChainedErrorHandler() {
        ErrorHandler handler = (c,s,e) -> System.out.println(e);
        Options o = new Options.Builder().errorHandler(handler).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained error handler", handler, o.getErrorHandler());
    }

    @Test
    public void testChainedConnectionHandler() {
        ConnectionHandler dcHandler = (c,e) -> System.out.println("discconnected" + e);
        ConnectionHandler rcHandler = (c,e) -> System.out.println("reconnected" + e);
        ConnectionHandler cHandler = (c,e) -> System.out.println("closed" + e);
        Options o = new Options.Builder().disconnectHandler(dcHandler).reconnectHandler(rcHandler).closeHandler(cHandler).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNull("error handler", o.getErrorHandler());
        assertTrue("chained disconnect handler", dcHandler == o.getDisconnectHandler());
        assertTrue("chained reconnect handler", rcHandler == o.getReconnectHandler());
        assertTrue("chained close handler", cHandler == o.getCloseHandler());
        assertTrue("chained close=disconnect", o.getCloseHandler() != o.getDisconnectHandler());
        assertTrue("chained reconnect=disconnect", o.getReconnectHandler() != o.getDisconnectHandler());
        assertTrue("chained close=reconnect", o.getReconnectHandler() != o.getCloseHandler());
    }

    @Test
    public void testPropertiesBooleanBuilder() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_VERBOSE, "true");
        props.setProperty(Options.PROP_PEDANTIC, "true");
        props.setProperty(Options.PROP_TLS_DEBUG, "true");
        props.setProperty(Options.PROP_NORANDOMIZE, "true");
        props.setProperty(Options.PROP_USE_OLD_REQUEST_STYLE, "true");

        Options o = new Options.Builder(props).build();
        assertNull("default username", o.getUsername());
        assertEquals("property verbose", true, o.isVerbose());
        assertEquals("property pedantic", true, o.isPedantic());
        assertEquals("property tlsDebug", true, o.isTlsDebug());
        assertEquals("property norandomize", true, o.isNoRandomize());
        assertEquals("property oldstyle", true, o.isOldRequestStyle());
    }
    
    @Test
    public void testPropertiesStringOptions() throws NoSuchAlgorithmException {
        Properties props = new Properties();
        props.setProperty(Options.PROP_USERNAME, "hello");
        props.setProperty(Options.PROP_PASSWORD, "world");
        props.setProperty(Options.PROP_TOKEN, "token");
        props.setProperty(Options.PROP_CONNECTION_NAME, "name");

        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("property username", "hello", o.getUsername());
        assertEquals("property password", "world", o.getPassword());
        assertEquals("property token", "token", o.getToken());
        assertEquals("property connection name", "name", o.getConnectionName());
    }

    @Test
    public void testPropertiesSSLOptions() throws NoSuchAlgorithmException {
        
        Properties props = new Properties();
        props.setProperty(Options.PROP_SECURE, "true");
        
        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNotNull("property context", o.getSslContext());
    }

    @Test
    public void testPropertyIntOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_MAX_RECONNECT, "100");
        props.setProperty(Options.PROP_MAX_PINGS, "200");
        props.setProperty(Options.PROP_RECONNECT_BUF_SIZE, "300");
        
        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("property max reconnect", 100, o.getMaxReconnect());
        assertEquals("property ping max", 200, o.getMaxPingsOut());
        assertEquals("property reconnect buffer size", 300, o.getReconnectBufferSize());
    }

    @Test
    public void testPropertyDurationOptions() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_RECONNECT_WAIT, "101");
        props.setProperty(Options.PROP_CONNECTION_TIMEOUT, "202");
        props.setProperty(Options.PROP_PING_INTERVAL, "303");
        
        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertEquals("chained reconnect wait", Duration.ofMillis(101), o.getReconnectWait());
        assertEquals("chained connection timeout", Duration.ofMillis(202), o.getConnectionTimeout());
        assertEquals("chained ping interval", Duration.ofMillis(303), o.getPingInterval());
    }
    
    @Test
    public void testPropertyErrorHandler() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_EXCEPTION_HANDLER, TestHandler.class.getCanonicalName());
        
        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNotNull("property error handler", o.getErrorHandler());

        o.getErrorHandler().errorOccurred(null, null, Errors.ERR_BAD_SUBJECT);
        assertEquals("property error handler class", ((TestHandler)o.getErrorHandler()).getCount(), 1);
    }
    
    @Test
    public void testPropertyConnectionHandlers() {
        Properties props = new Properties();
        props.setProperty(Options.PROP_DISCONNECTED_CB, TestHandler.class.getCanonicalName());
        props.setProperty(Options.PROP_RECONNECTED_CB, TestHandler.class.getCanonicalName());
        props.setProperty(Options.PROP_CLOSED_CB, TestHandler.class.getCanonicalName());
        
        Options o = new Options.Builder(props).build();
        assertEquals("default verbose", false, o.isVerbose()); // One from a different type
        assertNotNull("property disconnect handler", o.getDisconnectHandler());
        assertNotNull("property reconnect handler", o.getReconnectHandler());
        assertNotNull("property close handler", o.getCloseHandler());

        o.getDisconnectHandler().connectionEvent(null, Events.DISCONNECTED);
        o.getReconnectHandler().connectionEvent(null, Events.RECONNECTED);
        o.getReconnectHandler().connectionEvent(null, Events.RECONNECTED);
        o.getCloseHandler().connectionEvent(null, Events.CONNECTION_CLOSED);
        o.getCloseHandler().connectionEvent(null, Events.CONNECTION_CLOSED);
        o.getCloseHandler().connectionEvent(null, Events.CONNECTION_CLOSED);

        assertEquals("property disconnect handler class", ((TestHandler)o.getDisconnectHandler()).getCount(), 1);
        assertEquals("property disconnect handler class", ((TestHandler)o.getReconnectHandler()).getCount(), 2);
        assertEquals("property close handler class", ((TestHandler)o.getCloseHandler()).getCount(), 3);
    }
}