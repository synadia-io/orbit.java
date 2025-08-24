// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import io.synadia.counter.CounterContext;
import io.synadia.counter.CounterEntry;
import io.synadia.counter.CounterValue;

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

            System.out.println("\n3: getEntry() - The full CounterEntry for a subject, notice the last increment...");
            System.out.println("  getEntry(\"" + SUBJECT_A + "\") -> " + counter.getEntry(SUBJECT_A));
            System.out.println("  getEntry(\"" + SUBJECT_B + "\") -> " + counter.getEntry(SUBJECT_B));
            System.out.println("  getEntry(\"" + SUBJECT_C + "\") -> " + counter.getEntry(SUBJECT_C));

            System.out.println("\n4.1: getValues() - Get the CounterValue object for multiple subjects. Maybe to total them up?\"");
            LinkedBlockingQueue<CounterValue> qv = counter.getValues(SUBJECT_A, SUBJECT_B, SUBJECT_C);
            BigInteger total = BigInteger.ZERO;
            CounterValue value = qv.poll(1, TimeUnit.SECONDS);
            while (value != null && value.isValue()) {
                System.out.println("  " + value);
                total = total.add(value.value);
                value = qv.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  The iteration is signaled done when the CounterValue is a status: " + value);
            System.out.println("  Values totaled: " + total);

            System.out.println("\n4.2: getEntries() - Get CounterEntry for multiple subjects.");
            LinkedBlockingQueue<CounterEntry> qe = counter.getEntries(SUBJECT_A, SUBJECT_B, SUBJECT_C);
            CounterEntry entry = qe.poll(1, TimeUnit.SECONDS);
            while (entry != null && entry.isEntry()) {
                System.out.println("  " + entry);
                entry = qe.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  CounterEntry status 204 signals no more entries: " + entry);

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

            System.out.println("\n7.1: getValues() - Get multiple CounterValue - but no subjects have counters");
            qv = counter.getValues("no-counters", "also-counters");
            value = qv.poll(1, TimeUnit.SECONDS);
            while (value != null && value.isValue()) {
                System.out.println("  " + value);
                value = qv.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  CounterValue status 204 signals no more entries: " + value);

            System.out.println("\n7.2: getEntries() - Get multiple CounterEntry - but no subjects have counters");
            qe = counter.getEntries("no-counters", "also-counters");
            entry = qe.poll(1, TimeUnit.SECONDS);
            while (entry != null && entry.isEntry()) {
                System.out.println("  " + entry);
                entry = qe.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  The only CounterEntry received was a 404: " + entry);

            System.out.println("\n8.1: getValues() - Get multiple CounterValue - some subjects have any counters");
            qv = counter.getValues("no-counters", SUBJECT_A, SUBJECT_B, SUBJECT_C);
            value = qv.poll(1, TimeUnit.SECONDS);
            while (value != null && value.isValue()) {
                System.out.println("  " + value);
                value = qv.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  CounterValue status 204 signals no more entries: " + value);

            System.out.println("\n8.2: getEntries() - Get multiple CounterEntry - some subjects have any counters");
            qe = counter.getEntries("no-counters", SUBJECT_A, SUBJECT_B, SUBJECT_C);
            entry = qe.poll(1, TimeUnit.SECONDS);
            while (entry != null && entry.isEntry()) {
                System.out.println("  " + entry);
                entry = qe.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  CounterEntry status 204 signals no more entries: " + entry);
        }
    }
}
