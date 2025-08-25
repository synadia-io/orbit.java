// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import io.synadia.counter.CounterContext;
import io.synadia.counter.CounterEntryResponse;
import io.synadia.counter.CounterValueResponse;

import java.math.BigInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CounterContextExample {

    private static final String STREAM_NAME = "counter-stream";
    private static final String STREAM_SUBJECT = "cs.*";
    private static final String SUBJECT_A = "cs.A";
    private static final String SUBJECT_B = "cs.B";
    private static final String SUBJECT_C = "cs.C";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh counter stream
            try { jsm.deleteStream(STREAM_NAME); }  catch (JetStreamApiException ignore) {}
            CounterContext counter = CounterContext.createCounterStream(nc,
                StreamConfiguration.builder()
                    .name(STREAM_NAME)
                    .subjects(STREAM_SUBJECT)
                    .build());

            System.out.println("1: Add to a subject...");
            System.out.println("  add(\"" + SUBJECT_A + "\", 1) -> " + counter.add(SUBJECT_A, 1));
            System.out.println("  add(\"" + SUBJECT_A + "\", 2) -> " + counter.add(SUBJECT_A, 2));
            System.out.println("  add(\"" + SUBJECT_A + "\", 3) -> " + counter.add(SUBJECT_A, 3));
            System.out.println("  add(\"" + SUBJECT_A + "\", -1) -> " + counter.add(SUBJECT_A, -1));

            System.out.println("  add(\"" + SUBJECT_B + "\", 10) -> " + counter.add(SUBJECT_B, 10));
            System.out.println("  add(\"" + SUBJECT_B + "\", 20) -> " + counter.add(SUBJECT_B, 20));
            System.out.println("  add(\"" + SUBJECT_B + "\", 30) -> " + counter.add(SUBJECT_B, 30));
            System.out.println("  add(\"" + SUBJECT_B + "\", -10) -> " + counter.add(SUBJECT_B, -10));

            System.out.println("  add(\"" + SUBJECT_C + "\", 100) -> " + counter.add(SUBJECT_C, 100));
            System.out.println("  add(\"" + SUBJECT_C + "\", 200) -> " + counter.add(SUBJECT_C, 200));
            System.out.println("  add(\"" + SUBJECT_C + "\", 300) -> " + counter.add(SUBJECT_C, 300));
            System.out.println("  add(\"" + SUBJECT_C + "\", -100) -> " + counter.add(SUBJECT_C, -100));

            System.out.println("\n2.1: get() for existing subjects");
            System.out.println("  get(\"" + SUBJECT_A + "\") -> " + counter.get(SUBJECT_A));
            System.out.println("  get(\"" + SUBJECT_B + "\") -> " + counter.get(SUBJECT_B));
            System.out.println("  get(\"" + SUBJECT_C + "\") -> " + counter.get(SUBJECT_C));

            System.out.println("\n2.2: get() when the subject is not found");
            try {
                counter.get("not-found");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"not-found\") -> " + e);
            }

            System.out.println("\n2.3: get() with a default when the subject is not found");
            System.out.println("  get(\"\"not-found\", BigInteger.ZERO\") -> " + counter.getOrElse("not-found", BigInteger.ZERO));

            System.out.println("\n3: getEntry() - The full CounterEntry for a subject, notice the last increment...");
            System.out.println("  getEntry(\"" + SUBJECT_A + "\") -> " + counter.getEntry(SUBJECT_A));
            System.out.println("  getEntry(\"" + SUBJECT_B + "\") -> " + counter.getEntry(SUBJECT_B));
            System.out.println("  getEntry(\"" + SUBJECT_C + "\") -> " + counter.getEntry(SUBJECT_C));

            System.out.println("\n4.1: getMany() - Get the CounterValue/Response object for multiple subjects. Maybe to total them up?\"");
            LinkedBlockingQueue<CounterValueResponse> vResponses = counter.getMany(SUBJECT_A, SUBJECT_B, SUBJECT_C);
            BigInteger total = BigInteger.ZERO;
            CounterValueResponse vr = vResponses.poll(1, TimeUnit.SECONDS);
            while (vr != null && vr.isValue()) {
                System.out.println("  " + vr);
                total = total.add(vr.getValue());
                vr = vResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  The iteration is signaled done when the CounterValueResponse is a status: " + vr);
            System.out.println("  Values totaled: " + total);

            System.out.println("\n4.2: getEntries() - Get CounterEntry/Response for multiple subjects.");
            LinkedBlockingQueue<CounterEntryResponse> responses = counter.getEntries(SUBJECT_A, SUBJECT_B, SUBJECT_C);
            CounterEntryResponse entry = responses.poll(1, TimeUnit.SECONDS);
            while (entry != null && entry.isEntry()) {
                System.out.println("  " + entry);
                entry = responses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  CounterEntryResponse status EOB signals no more entries: " + entry);

            System.out.println("\n5.1: set() - Set the value for a subject");
            System.out.println("  set(\"" + SUBJECT_A + "\", 9) -> " + counter.set(SUBJECT_A, 9));
            System.out.println("  set(\"" + SUBJECT_B + "\", 99) -> " + counter.set(SUBJECT_B, 99));
            System.out.println("  set(\"" + SUBJECT_C + "\", 999) -> " + counter.set(SUBJECT_C, 999));

            System.out.println("\n5.2: getEntry() - Get the full CounterEntry, notice the last increment after a set represents" +
                               "\n                  the difference between the entry before the set and the set value.");
            System.out.println("  getEntry(\"" + SUBJECT_A + "\") -> " + counter.getEntry(SUBJECT_A));
            System.out.println("  getEntry(\"" + SUBJECT_B + "\") -> " + counter.getEntry(SUBJECT_B));
            System.out.println("  getEntry(\"" + SUBJECT_C + "\") -> " + counter.getEntry(SUBJECT_C));

            System.out.println("\n6.1: zero() is a shortcut to set the value of a subject to 0");
            System.out.println("  zero(\"" + SUBJECT_A + "\") -> " + counter.zero(SUBJECT_A));
            System.out.println("  zero(\"" + SUBJECT_B + "\") -> " + counter.zero(SUBJECT_B));
            System.out.println("  zero(\"" + SUBJECT_C + "\") -> " + counter.zero(SUBJECT_C));

            System.out.println("\n6.2: getEntry() - Get the full CounterEntry, notice the last increment after a zero represents" +
                               "\n                  the difference between the entry before the zero and zero.");
            System.out.println("  getEntry(\"" + SUBJECT_A + "\") -> " + counter.getEntry(SUBJECT_A));
            System.out.println("  getEntry(\"" + SUBJECT_B + "\") -> " + counter.getEntry(SUBJECT_B));
            System.out.println("  getEntry(\"" + SUBJECT_C + "\") -> " + counter.getEntry(SUBJECT_C));

            System.out.println("\n7: getEntries() - Get multiple CounterEntry/Response - but no subjects have counters");
            responses = counter.getEntries("no-counters", "also-counters");
            entry = responses.poll(1, TimeUnit.SECONDS);
            while (entry != null && entry.isEntry()) {
                System.out.println("  " + entry);
                entry = responses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  The only CounterEntryResponse received was a 404: " + entry);

            System.out.println("\n8: getEntries() - Get multiple CounterEntry/Response - some subjects have any counters");
            responses = counter.getEntries("no-counters", SUBJECT_A, SUBJECT_B, SUBJECT_C);
            entry = responses.poll(1, TimeUnit.SECONDS);
            while (entry != null && entry.isEntry()) {
                System.out.println("  " + entry);
                entry = responses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  CounterEntryResponse status EOB signals no more entries: " + entry);
        }
    }
}
