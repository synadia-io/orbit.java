// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.synadia.counter.Counter;
import io.synadia.counter.CounterEntryResponse;

import java.math.BigInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CounterExample {
    static final String NATS_URL = "nats://localhost:4222";

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect(NATS_URL)) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh counter stream
            try { jsm.deleteStream("counter-stream"); }  catch (JetStreamApiException ignore) {}
            Counter counter = Counter.createCounterStream(nc,
                StreamConfiguration.builder()
                    .name("counter-stream")
                    .subjects("cs.*")
                    .storageType(StorageType.Memory)
                    .build());

            // ----------------------------------------------------------------------------------------------------
            System.out.println("1.1: Add to a subject...");
            System.out.println(" add(\"cs.A\", 1) -> " + counter.add("cs.A", 1));
            System.out.println(" add(\"cs.A\", 2) -> " + counter.add("cs.A", 2));
            System.out.println(" add(\"cs.A\", 3) -> " + counter.add("cs.A", 3));
            System.out.println(" add(\"cs.A\", -1) -> " + counter.add("cs.A", -1));

            System.out.println(" add(\"cs.B\", 10) -> " + counter.add("cs.B", 10));
            System.out.println(" add(\"cs.B\", 20) -> " + counter.add("cs.B", 20));
            System.out.println(" add(\"cs.B\", 30) -> " + counter.add("cs.B", 30));
            System.out.println(" add(\"cs.B\", -10) -> " + counter.add("cs.B", -10));

            System.out.println(" add(\"cs.C\", 100) -> " + counter.add("cs.C", 100));
            System.out.println(" add(\"cs.C\", 200) -> " + counter.add("cs.C", 200));
            System.out.println(" add(\"cs.C\", 300) -> " + counter.add("cs.C", 300));
            System.out.println(" add(\"cs.C\", -100) -> " + counter.add("cs.C", -100));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n2.1: get() for existing subjects");
            System.out.println(" get(\"cs.A\") -> " + counter.get("cs.A"));
            System.out.println(" get(\"cs.B\") -> " + counter.get("cs.B"));
            System.out.println(" get(\"cs.C\") -> " + counter.get("cs.C"));

            System.out.println("\n2.2: get() when the subject is not found");
            try {
                counter.get("cs.not-found");
            }
            catch (JetStreamApiException e) {
                System.out.println(" get(\"cs.not-found\") -> " + e);
            }

            System.out.println("\n2.3: get() for a single subject does not allow wildcards");
            try {
                counter.get("cs.*");
            }
            catch (IllegalArgumentException e) {
                System.out.println(" get(\"cs.*\") -> " + e);
            }

            System.out.println("\n2.4: getOrElse() with a default when the subject is found");
            System.out.println(" getOrElse(\"cs.C\", BigInteger.ZERO\") -> " + counter.getOrElse("cs.C", BigInteger.ZERO));

            System.out.println("\n2.5: getOrElse() with a default when the subject is not found");
            try {
                counter.get("cs.not-found");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"cs.not-found\") -> " + e);
            }
            System.out.println("  getOrElse(\"cs.not-found\", 77777) -> " + counter.getOrElse("cs.not-found", 77777));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n3.1: getEntry() - The full CounterEntry for a subject, notice the last increment...");
            System.out.println(" getEntry(\"cs.A\") -> " + counter.getEntry("cs.A"));
            System.out.println(" getEntry(\"cs.B\") -> " + counter.getEntry("cs.B"));
            System.out.println(" getEntry(\"cs.C\") -> " + counter.getEntry("cs.C"));

            System.out.println("\n3.2: getEntry() does not allow wildcards");
            try {
                counter.getEntry("cs.>");
            }
            catch (IllegalArgumentException e) {
                System.out.println(" getEntry(\"cs.>\") -> " + e);
            }

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n4.1: getEntries(\"cs.A\", \"cs.B\", \"cs.C\") - Get the CounterEntryResponse objects for multiple subjects.");
            LinkedBlockingQueue<CounterEntryResponse> eResponses = counter.getEntries("cs.A", "cs.B", "cs.C");
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
            eResponses = counter.getEntries("cs.*");
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
            System.out.println(" setViaAdd(\"cs.A\", 9) -> " + counter.setViaAdd("cs.A", 9));
            System.out.println(" setViaAdd(\"cs.B\", 99) -> " + counter.setViaAdd("cs.B", 99));
            System.out.println(" setViaAdd(\"cs.C\", 999) -> " + counter.setViaAdd("cs.C", 999));

            System.out.println("\n5.2: getEntry() - Get the full CounterEntry, notice the last increment after a setViaAdd" +
                               "\n     represents the difference between the entry before the set and the set value.");
            System.out.println(" getEntry(\"cs.A\") -> " + counter.getEntry("cs.A"));
            System.out.println(" getEntry(\"cs.B\") -> " + counter.getEntry("cs.B"));
            System.out.println(" getEntry(\"cs.C\") -> " + counter.getEntry("cs.C"));

            System.out.println("\n5.3: It's safe to call setViaAdd() even if the subject did not exist because it uses getOrElse;");
            try {
                counter.get("cs.did-not-exist");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"cs.did-not-exist\") -> " + e);
            }
            System.out.println("  setViaAdd(\"cs.did-not-exist\", 99999) -> " + counter.setViaAdd("cs.did-not-exist", 99999));
            System.out.println("  get(\"cs.did-not-exist\") -> " + counter.get("cs.did-not-exist"));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n6.1: getEntries(\"cs.no-counters\", \"cs.also-counters\") - getEntries but no subjects have counters.");
            eResponses = counter.getEntries("cs.no-counters", "cs.also-counters");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er);

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n7.1: getEntries(\"no-counters\", \"cs.A\", \"cs.B\", \"cs.C\") - getEntries when some subjects have counters.");
            eResponses = counter.getEntries("cs.no-counters", "cs.A", "cs.B", "cs.C");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er + " -> No more entries.");
        }
    }
}
