// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.counters;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;
import io.synadia.direct.DirectBatchContext;
import io.synadia.direct.MessageBatchGetRequest;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static io.nats.client.support.Validator.required;
import static io.synadia.counters.CountersUtils.INCREMENT_HEADER;
import static io.synadia.counters.CountersUtils.extractVal;

public class Counters {

    public static Counters createCountersStream(Connection conn, StreamConfiguration userConfig) throws JetStreamApiException, IOException {
        return createCountersStream(conn, null, userConfig);
    }

    public static Counters createCountersStream(Connection conn, JetStreamOptions jso, StreamConfiguration userConfig) throws JetStreamApiException, IOException {
        if (userConfig.getRetentionPolicy() != RetentionPolicy.Limits) {
            throw new IllegalArgumentException("Retention Policy - Limits is the only allowed limit for counter streams.");
        }
        if (userConfig.getDiscardPolicy() == DiscardPolicy.New) {
            throw new IllegalArgumentException("Discard Policy - New is not allowed for counter streams.");
        }
        StreamConfiguration config = StreamConfiguration.builder(userConfig)
            .allowDirect(true)
            .allowMessageCounter(true)
            .build();

        JetStreamManagement jsm = conn.jetStreamManagement(jso);
        StreamInfo si = jsm.addStream(config);

        return new Counters(config.getName(), conn, jso, jsm, si);
    }

    private final String streamName;
    private final Connection conn;
    private final Duration timeout;
    private final JetStreamManagement jsm;
    private final JetStream js;
    private final DirectBatchContext dbCtx;

    public Counters(String streamName, Connection conn) throws IOException, JetStreamApiException {
        this(streamName, conn, null, null, null);
    }

    public Counters(String streamName, Connection conn, JetStreamOptions jso) throws IOException, JetStreamApiException {
        this(streamName, conn, jso, null, null);
    }

    private Counters(@NonNull String streamName,
                     @NonNull Connection conn,
                     @Nullable JetStreamOptions jso,
                     @Nullable JetStreamManagement jsm,
                     @Nullable StreamInfo si
    ) throws IOException, JetStreamApiException
    {
        this.conn = conn;

        Duration tempTimeout = null;
        if (jso != null) {
            tempTimeout = jso.getRequestTimeout();
        }
        if (tempTimeout == null) {
            tempTimeout = conn.getOptions().getConnectionTimeout();
        }
        timeout = tempTimeout;

        this.jsm = jsm == null ? conn.jetStreamManagement(jso) : jsm;
        js = this.jsm.jetStream();

        if (si == null) {
            this.streamName = required(streamName, "Stream name required,");
            si = this.jsm.getStreamInfo(streamName);
        }
        else {
            this.streamName = si.getConfiguration().getName();
        }

        if (!si.getConfiguration().getAllowDirect()) {
            throw new IllegalArgumentException("Stream must have allow direct set.");
        }

        if (!si.getConfiguration().getAllowMessageCounter()) {
            throw new IllegalArgumentException("Stream must have allow message counter set.");
        }

        dbCtx = new DirectBatchContext(conn, jso, streamName, si);
    }

    private BigInteger _add(String subject, String sv) throws IOException, JetStreamApiException {
        validateSingleSubject(subject);
        Headers h = new Headers();
        h.put(INCREMENT_HEADER, sv);
        PublishAck pa = js.publish(subject, h, null);
        String val = pa.getVal();
        if (val == null) {
            throw new IOException("Publish Failed");
        }
        return new BigInteger(val);
    }

    public BigInteger add(String subject, int value) throws JetStreamApiException, IOException {
        return _add(subject, Integer.toString(value));
    }

    public BigInteger add(String subject, long value) throws JetStreamApiException, IOException {
        return _add(subject, Long.toString(value));
    }

    public BigInteger add(String subject, BigInteger value) throws JetStreamApiException, IOException {
        return _add(subject, value.toString());
    }

    public BigInteger increment(String subject) throws JetStreamApiException, IOException {
        return _add(subject, "1");
    }

    public BigInteger decrement(String subject) throws JetStreamApiException, IOException {
        return _add(subject, "-1");
    }

    public BigInteger setViaAdd(String subject, int value) throws JetStreamApiException, IOException {
        return setViaAdd(subject, BigInteger.valueOf(value));
    }

    public BigInteger setViaAdd(String subject, long value) throws JetStreamApiException, IOException {
        return setViaAdd(subject, BigInteger.valueOf(value));
    }

    public BigInteger setViaAdd(String subject, BigInteger value) throws JetStreamApiException, IOException {
        BigInteger bi = getOrElse(subject, BigInteger.ZERO);
        return _add(subject, value.subtract(bi).toString());
    }

    public BigInteger get(String subject) throws JetStreamApiException, IOException {
        validateSingleSubject(subject);
        MessageInfo mi = jsm.getMessage(streamName, MessageGetRequest.lastForSubject(subject));
        return extractVal(mi.getData());
    }

    public BigInteger getOrElse(String subject, int dflt) throws IOException {
        return getOrElse(subject, BigInteger.valueOf(dflt));
    }

    public BigInteger getOrElse(String subject, long dflt) throws IOException {
        return getOrElse(subject, BigInteger.valueOf(dflt));
    }

    public BigInteger getOrElse(String subject, BigInteger dflt) throws IOException {
        try {
            return get(subject);
        }
        catch (JetStreamApiException e) {
            return dflt;
        }
    }

    public CounterEntry getEntry(String subject) throws JetStreamApiException, IOException {
        validateSingleSubject(subject);
        MessageInfo mi = jsm.getLastMessage(streamName, subject);
        return new CounterEntry(mi);
    }

    public LinkedBlockingQueue<CounterEntryResponse> getEntries(String... subjects) {
        return getEntries(Arrays.asList(subjects));
    }

    public LinkedBlockingQueue<CounterEntryResponse> getEntries(List<String> subjects) {
        LinkedBlockingQueue<CounterEntryResponse> queue = new LinkedBlockingQueue<>();
        MessageBatchGetRequest mbgr = MessageBatchGetRequest.multiLastForSubjects(subjects);
        conn.getOptions().getExecutor().submit(
            () -> dbCtx.requestMessageBatch(mbgr, mi -> queue.add(new CounterEntryResponse(mi))));
        return queue;
    }

    public CounterIterator iterateEntries(String... subjects) {
        return new CounterIterator(getEntries(Arrays.asList(subjects)), timeout);
    }

    public CounterIterator iterateEntries(List<String> subjects) {
        return new CounterIterator(getEntries(subjects), timeout);
    }

    public CounterIterator iterateEntries(List<String> subjects, Duration timeoutFirst, Duration timeoutSubsequent) {
        return new CounterIterator(getEntries(subjects), timeoutFirst, timeoutSubsequent);
    }

    private static void validateSingleSubject(String subject) {
        if (subject == null || subject.isEmpty()) {
            throw new IllegalArgumentException("Subject required.");
        }
        if (subject.contains("*") || subject.contains(">")) {
            throw new IllegalArgumentException("Subject must not contain wildcards '*' or '>'.");
        }
    }
}
