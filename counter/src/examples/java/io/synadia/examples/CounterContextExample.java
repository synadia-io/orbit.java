// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import io.synadia.counter.Counter;
import io.synadia.counter.CounterEntryResponse;
import io.synadia.counter.CounterValueResponse;

import java.math.BigInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CounterContextExample {

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh counter stream
            try { jsm.deleteStream("counter-stream"); }  catch (JetStreamApiException ignore) {}
            Counter counter = Counter.createCounterStream(nc,
                StreamConfiguration.builder()
                    .name("counter-stream")
                    .subjects("cs.*")
                    .build());

            // ----------------------------------------------------------------------------------------------------
            System.out.println("1: Add to a subject...");
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
                counter.get("not-found");
            }
            catch (JetStreamApiException e) {
                System.out.println(" get(\"not-found\") -> " + e);
            }

            System.out.println("\n2.3: get() for a single subject does not allow wildcards");
            try {
                counter.get("cs.*");
            }
            catch (RuntimeException e) {
                System.out.println(" get(\"cs.*\") -> " + e);
            }

            System.out.println("\n2.3: get() with a default when the subject is not found");
            System.out.println(" get(\"not-found\", BigInteger.ZERO\") -> " + counter.getOrElse("not-found", BigInteger.ZERO));

            System.out.println("\n2.4: get() with a default when the subject IS found");
            System.out.println(" get(\"cs.C\", BigInteger.ZERO\") -> " + counter.getOrElse("cs.C", BigInteger.ZERO));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n3: getEntry() - The full CounterEntry for a subject, notice the last increment...");
            System.out.println(" getEntry(\"cs.A\") -> " + counter.getEntry("cs.A"));
            System.out.println(" getEntry(\"cs.B\") -> " + counter.getEntry("cs.B"));
            System.out.println(" getEntry(\"cs.C\") -> " + counter.getEntry("cs.C"));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n4.1: getMany(\"cs.A\", \"cs.B\", \"cs.C\") - Get the CounterValue/Response object for multiple subjects. Maybe to total them up?\"");
            LinkedBlockingQueue<CounterValueResponse> vResponses = counter.getValues("cs.A", "cs.B", "cs.C");
            BigInteger total = BigInteger.ZERO;
            CounterValueResponse vr = vResponses.poll(1, TimeUnit.SECONDS);
            while (vr != null && vr.isValue()) {
                System.out.println(" " + vr);
                total = total.add(vr.getValue());
                vr = vResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + vr + " -> No more entries.");
            System.out.println(" Values totaled: " + total);

            System.out.println("\n4.2: getEntries(\"cs.A\", \"cs.B\", \"cs.C\") - Get CounterEntry/Response for multiple subjects.");
            LinkedBlockingQueue<CounterEntryResponse> eResponses = counter.getEntries("cs.A", "cs.B", "cs.C");
            CounterEntryResponse er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er + " -> No more entries.");

            System.out.println("\n4.3: getMany(\"cs.*\") - Get the CounterValue/Response for wildcard subject(s).");
            vResponses = counter.getValues("cs.*");
            vr = vResponses.poll(1, TimeUnit.SECONDS);
            while (vr != null && vr.isValue()) {
                System.out.println(" " + vr);
                vr = vResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + vr + " -> No more entries.");

            System.out.println("\n4.4: getEntries(\"cs.*\") - Get CounterEntry/Response for wildcard subject(s).");
            eResponses = counter.getEntries("cs.*");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er + " -> No more entries.");

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n5.1: set() - Set the value for a subject");
            System.out.println(" set(\"cs.A\", 9) -> " + counter.set("cs.A", 9));
            System.out.println(" set(\"cs.B\", 99) -> " + counter.set("cs.B", 99));
            System.out.println(" set(\"cs.C\", 999) -> " + counter.set("cs.C", 999));

            System.out.println("\n5.2: getEntry() - Get the full CounterEntry, notice the last increment after a set represents" +
                               "\n     the difference between the entry before the set and the set value.");
            System.out.println(" getEntry(\"cs.A\") -> " + counter.getEntry("cs.A"));
            System.out.println(" getEntry(\"cs.B\") -> " + counter.getEntry("cs.B"));
            System.out.println(" getEntry(\"cs.C\") -> " + counter.getEntry("cs.C"));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n6.1: zero() is a shortcut to set the value of a subject to 0.");
            System.out.println(" zero(\"cs.A\") -> " + counter.zero("cs.A"));
            System.out.println(" zero(\"cs.B\") -> " + counter.zero("cs.B"));
            System.out.println(" zero(\"cs.C\") -> " + counter.zero("cs.C"));

            System.out.println("\n6.2: getEntry() - Get the full CounterEntry, notice the last increment after a zero represents" +
                               "\n     the difference between the entry before the zero() and 0.");
            System.out.println(" getEntry(\"cs.A\") -> " + counter.getEntry("cs.A"));
            System.out.println(" getEntry(\"cs.B\") -> " + counter.getEntry("cs.B"));
            System.out.println(" getEntry(\"cs.C\") -> " + counter.getEntry("cs.C"));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n7: getEntries(\"no-counters\", \"also-counters\") - Get multiple but no subjects have counters.");
            eResponses = counter.getEntries("no-counters", "also-counters");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er);

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n8: getEntries(\"no-counters\", \"cs.A\", \"cs.B\", \"cs.C\") - Get multiple when some subjects have counters.");
            eResponses = counter.getEntries("no-counters", "cs.A", "cs.B", "cs.C");
            er = eResponses.poll(1, TimeUnit.SECONDS);
            while (er != null && er.isEntry()) {
                System.out.println(" " + er);
                er = eResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println(" " + er + " -> No more entries.");

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n9: getOrElse() returns the 'else' value if the subject is not found.");
            try {
                counter.get("or-else");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"or-else\") -> " + e);
            }
            System.out.println("  getOrElse(\"or-else\", 77777) -> " + counter.getOrElse("or-else", 77777));

            // ----------------------------------------------------------------------------------------------------
            System.out.println("\n10: It's safe to call set() even if the subject did not exist.");
            try {
                counter.get("cs.X");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"cs.X\") -> " + e);
            }
            System.out.println("  set(\"cs.X\", 99999) -> " + counter.add("cs.X", 99999));
            System.out.println("  get(\"cs.X\") -> " + counter.get("cs.X"));
        }
    }
}
