// Copyright (c) 2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.bp;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.api.ServerInfo;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The publishers have different minimum server versions and each refuses to build against
 * a server that is too old. A real server cannot cover this — the test server is always current,
 * so the too-old branch would never execute. Connection is an interface and ServerInfo parses
 * from the INFO json, so a proxy can claim to be any version.
 */
public class ServerVersionGateTests {

    private static Connection connectionReporting(String version) {
        String infoJson = "{"
            + "\"server_id\":\"TESTSERVERID\","
            + "\"server_name\":\"test\","
            + "\"version\":\"" + version + "\","
            + "\"proto\":1,"
            + "\"go\":\"go1.22\","
            + "\"host\":\"127.0.0.1\","
            + "\"port\":4222,"
            + "\"headers\":true,"
            + "\"max_payload\":1048576"
            + "}";
        ServerInfo si = new ServerInfo(infoJson);
        assertEquals(version, si.getVersion(), "the fake ServerInfo must actually report the version");

        return (Connection) Proxy.newProxyInstance(
            ServerVersionGateTests.class.getClassLoader(),
            new Class<?>[]{Connection.class},
            (proxy, method, args) -> {
                switch (method.getName()) {
                    case "getServerInfo":
                        return si;
                    case "createInbox":
                        return "_INBOX.versiongatetest";
                    case "subscribe":
                        return null;   // never published on; build() is as far as these tests go
                    case "createDispatcher":
                        // the fast publishers subscribe their control channel at construction
                        return Proxy.newProxyInstance(
                            ServerVersionGateTests.class.getClassLoader(),
                            new Class<?>[]{Dispatcher.class},
                            (d, dm, da) -> {
                                switch (dm.getName()) {
                                    case "toString":
                                        return "FakeDispatcher";
                                    case "hashCode":
                                        return System.identityHashCode(d);
                                    case "equals":
                                        return d == da[0];
                                    default:
                                        return null;
                                }
                            });
                    case "toString":
                        return "FakeConnection[" + version + "]";
                    case "hashCode":
                        return System.identityHashCode(proxy);
                    case "equals":
                        return proxy == args[0];
                    default:
                        return null;
                }
            });
    }

    // an explicit ackTimeout keeps build() from reaching for conn.getOptions()
    private static final long ACK_TIMEOUT = 5000;

    // ----------------------------------------------------------------------------------
    // BatchPublisher - needs 2.12.0
    // ----------------------------------------------------------------------------------
    @Test
    public void testBatchPublisherRejectsPre212() {
        Connection old = connectionReporting("2.11.6");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> BatchPublisher.builder().connection(old).ackTimeout(ACK_TIMEOUT).build());
        assertTrue(e.getMessage().contains("2.12.0"), "message should name the required version: " + e.getMessage());
    }

    @Test
    public void testBatchPublisherAllowsFrom212() {
        assertNotNull(BatchPublisher.builder()
            .connection(connectionReporting("2.12.0")).ackTimeout(ACK_TIMEOUT).build());
        assertNotNull(BatchPublisher.builder()
            .connection(connectionReporting("2.13.5")).ackTimeout(ACK_TIMEOUT).build());
    }

    // ----------------------------------------------------------------------------------
    // EobBatchPublisher - needs 2.14.0, stricter than plain atomic
    // ----------------------------------------------------------------------------------
    @Test
    public void testEobBatchPublisherRejectsPre214() {
        // 2.13.5 is new enough for an atomic batch but not for an EOB commit. This is the whole
        // reason EOB is its own type: the batch is refused up front rather than after staging.
        Connection old = connectionReporting("2.13.5");
        assertNotNull(BatchPublisher.builder().connection(old).ackTimeout(ACK_TIMEOUT).build(),
            "2.13.5 must still be fine for a plain atomic batch");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> EobBatchPublisher.builder().connection(old).ackTimeout(ACK_TIMEOUT).build());
        assertTrue(e.getMessage().contains("2.14.0"), "message should name the required version: " + e.getMessage());
    }

    @Test
    public void testEobBatchPublisherAllowsFrom214() {
        assertNotNull(EobBatchPublisher.builder()
            .connection(connectionReporting("2.14.0")).ackTimeout(ACK_TIMEOUT).build());
        assertNotNull(EobBatchPublisher.builder()
            .connection(connectionReporting("2.15.0")).ackTimeout(ACK_TIMEOUT).build());
    }

    // ----------------------------------------------------------------------------------
    // FastPublisher - needs 2.14.0
    // ----------------------------------------------------------------------------------
    @Test
    public void testFastPublisherRejectsPre214() {
        // there is no server side error to fall back on here: a pre 2.14 server treats the $FI
        // reply subject as an ordinary reply and never answers, so without this gate the client
        // would hang until ackTimeout.
        Connection old = connectionReporting("2.13.5");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
            () -> FastPublisher.builder().connection(old).ackTimeout(ACK_TIMEOUT).build());
        assertTrue(e.getMessage().contains("2.14.0"), "message should name the required version: " + e.getMessage());
    }

    @Test
    public void testFastPublisherAllowsFrom214() {
        assertNotNull(FastPublisher.builder()
            .connection(connectionReporting("2.14.0")).ackTimeout(ACK_TIMEOUT).build());
    }

    // ----------------------------------------------------------------------------------
    // the gate must not reject a dev or release candidate build of a good version
    // ----------------------------------------------------------------------------------
    @Test
    public void testPrereleaseVersionsAccepted() {
        // the local test server reports 2.15.0-dev, so this shape has to keep working
        assertNotNull(FastPublisher.builder()
            .connection(connectionReporting("2.15.0-dev")).ackTimeout(ACK_TIMEOUT).build());
        assertNotNull(EobBatchPublisher.builder()
            .connection(connectionReporting("2.15.0-dev")).ackTimeout(ACK_TIMEOUT).build());
    }
}
