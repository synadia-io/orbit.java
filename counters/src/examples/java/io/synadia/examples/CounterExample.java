// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.counters.CounterEntryResponse;
import io.synadia.counters.CounterIterator;
import io.synadia.counters.Counters;

import java.math.BigInteger;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static io.synadia.counters.Counters.createCountersStream;

public class CounterExample {
    static final String NATS_URL = "nats://localhost:4222";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh counters stream
            try { jsm.deleteStream("counters-stream"); }  catch (JetStreamApiException ignore) {}
            Counters counters = createCountersStream(nc,
                StreamConfiguration.builder()
                    .name("counters-stream")
                    .subjects("cs.*")
                    .storageType(StorageType.Memory)
                    .build());

            // ----------------------------------------------------------------------------------------------------
            System.out.println("1.1: Add to a subject...");
            System.out.println(" add(\"cs.A\", 1) -> " + counters.add("cs.A", 1));
            System.out.println(" add(\"cs.A\", 2) -> " + counters.add("cs.A", 2));
            System.out.println(" add(\"cs.A\", 3) -> " + counters.add("cs.A", 3));
            System.out.println(" add(\"cs.A\", -1) -> " + counters.add("cs.A", -1));

            System.out.println(" add(\"cs.B\", 10) -> " + counters.add("cs.B", 10));
            System.out.println(" add(\"cs.B\", 20) -> " + counters.add("cs.B", 20));
            System.out.println(" add(\"cs.B\", 30) -> " + counters.add("cs.B", 30));
            System.out.println(" add(\"cs.B\", -10) -> " + counters.add("cs.B", -10));

            System.out.println(" add(\"cs.C\", 100) -> " + counters.add("cs.C", 100));
            System.out.println(" add(\"cs.C\", 200) -> " + counters.add("cs.C", 200));
            System.out.println(" add(\"cs.C\", 300) -> " + counters.add("cs.C", 300));
            System.out.println(" add(\"cs.C\", -100) -> " + counters.add("cs.C", -100));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n2.1: get() for existing subjects");
            System.out.println(" get(\"cs.A\") -> " + counters.get("cs.A"));
            System.out.println(" get(\"cs.B\") -> " + counters.get("cs.B"));
            System.out.println(" get(\"cs.C\") -> " + counters.get("cs.C"));

            System.out.println("\n2.2: get() when the subject is not found");
            try {
                counters.get("cs.not-found");
            }
            catch (JetStreamApiException e) {
                System.out.println(" get(\"cs.not-found\") -> " + e);
            }

            System.out.println("\n2.3: get() for a single subject does not allow wildcards");
            try {
                counters.get("cs.*");
            }
            catch (IllegalArgumentException e) {
                System.out.println(" get(\"cs.*\") -> " + e);
            }

            System.out.println("\n2.4: getOrElse() with a default when the subject is found");
            System.out.println(" getOrElse(\"cs.C\", BigInteger.ZERO\") -> " + counters.getOrElse("cs.C", BigInteger.ZERO));

            System.out.println("\n2.5: getOrElse() with a default when the subject is not found");
            try {
                counters.get("cs.not-found");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"cs.not-found\") -> " + e);
            }
            System.out.println("  getOrElse(\"cs.not-found\", 77777) -> " + counters.getOrElse("cs.not-found", 77777));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n3.1: getEntry() - The full CounterEntry for a subject, notice the last increment...");
            System.out.println(" getEntry(\"cs.A\") -> " + counters.getEntry("cs.A"));
            System.out.println(" getEntry(\"cs.B\") -> " + counters.getEntry("cs.B"));
            System.out.println(" getEntry(\"cs.C\") -> " + counters.getEntry("cs.C"));

            System.out.println("\n3.2: getEntry() does not allow wildcards");
            try {
                counters.getEntry("cs.>");
            }
            catch (IllegalArgumentException e) {
                System.out.println(" getEntry(\"cs.>\") -> " + e);
            }

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n4.1: getEntries(\"cs.A\", \"cs.B\", \"cs.C\") - Get the CounterEntryResponse objects for multiple subjects.");
            LinkedBlockingQueue<CounterEntryResponse> eResponses = counters.getEntries("cs.A", "cs.B", "cs.C");
            BigInteger total = BigInteger.ZERO;
            CounterEntryResponse er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                // the entry response has a method to simplify getting the value
                total = total.add(er.getValue());
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er + " -> No more entries.");
            System.out.println(" Values totaled: " + total);

            System.out.println("\n4.2: getEntries(\"cs.*\") - Get CounterEntryResponse objects for wildcard subject(s).");
            eResponses = counters.getEntries("cs.*");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er + " -> No more entries.");

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n5.1: setViaAdd() - Sets the value for a subject by" +
                               "\n     1) calling getOrElse(subject, BigInteger.ZERO)" +
                               "\n     2) then calling add with the set value minus the current value.");
            System.out.println(" setViaAdd(\"cs.A\", 9) -> " + counters.setViaAdd("cs.A", 9));
            System.out.println(" setViaAdd(\"cs.B\", 99) -> " + counters.setViaAdd("cs.B", 99));
            System.out.println(" setViaAdd(\"cs.C\", 999) -> " + counters.setViaAdd("cs.C", 999));

            System.out.println("\n5.2: getEntry() - Get the full CounterEntry, notice the last increment after a setViaAdd" +
                               "\n     represents the difference between the entry before the set and the set value.");
            System.out.println(" getEntry(\"cs.A\") -> " + counters.getEntry("cs.A"));
            System.out.println(" getEntry(\"cs.B\") -> " + counters.getEntry("cs.B"));
            System.out.println(" getEntry(\"cs.C\") -> " + counters.getEntry("cs.C"));

            System.out.println("\n5.3: It's safe to call setViaAdd() even if the subject did not exist because it uses getOrElse;");
            try {
                counters.get("cs.did-not-exist");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"cs.did-not-exist\") -> " + e);
            }
            System.out.println("  setViaAdd(\"cs.did-not-exist\", 99999) -> " + counters.setViaAdd("cs.did-not-exist", 99999));
            System.out.println("  get(\"cs.did-not-exist\") -> " + counters.get("cs.did-not-exist"));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n6.1: getEntries(\"cs.no-counters\", \"cs.also-counters\") - getEntries but no subjects have counters.");
            eResponses = counters.getEntries("cs.no-counters", "cs.also-counters");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er);

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n7.1: getEntries(\"no-counters\", \"cs.A\", \"cs.B\", \"cs.C\") - getEntries when some subjects have counters.");
            eResponses = counters.getEntries("cs.no-counters", "cs.A", "cs.B", "cs.C");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er + " -> No more entries.");

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n8.1: iterateEntries(\"cs.A\", \"cs.B\", \"cs.C\") - Get via CounterIterator for multiple subjects.");
            CounterIterator iterator = counters.iterateEntries("cs.A", "cs.B", "cs.C");
            while (iterator.hasNext()) {
                System.out.println(" " + iterator.next());
            }

            System.out.println("\n8.2: iterateEntries(\"cs.*\") - Get via CounterIterator for wildcard subject(s).");
            iterator = counters.iterateEntries("cs.*");
            while (iterator.hasNext()) {
                System.out.println(" " + iterator.next());
            }

            System.out.println("\n8.3: iterateEntries(\"cs.*\", timeoutFirst, timeoutSubsequent) - Get via CounterIterator with custom timeouts.");
            Duration timeoutFirst = Duration.ofMillis(1000);
            Duration timeoutSubsequent = Duration.ofMillis(200);
            iterator = counters.iterateEntries(Collections.singletonList("cs.*"), timeoutFirst, timeoutSubsequent);
            while (iterator.hasNext()) {
                System.out.println(" " + iterator.next());
            }
        }
    }
}
