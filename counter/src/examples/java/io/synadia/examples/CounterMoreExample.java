// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.StreamConfiguration;
import io.synadia.counter.CounterContext;
import io.synadia.counter.CounterValueResponse;

import java.math.BigInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CounterMoreExample {

    public static void main(String[] args) throws Exception {
        try (Connection nc = Nats.connect()) {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // Set up a fresh counter stream
            try { jsm.deleteStream("more-stream"); }  catch (JetStreamApiException ignore) {}
            CounterContext counter = CounterContext.createCounterStream(nc,
                StreamConfiguration.builder()
                    .name("more-stream")
                    .subjects("more.*")
                    .build());

            System.out.println("1: Add some values...");
            System.out.println("  add(\"more.A\", 5) -> " + counter.add("more.A", 5));
            System.out.println("  add(\"more.B\", 50) -> " + counter.add("more.B", 50));
            System.out.println("  add(\"more.C\", 500) -> " + counter.add("more.C", 500));
            System.out.println("  add(\"more.D\", 5000) -> " + counter.add("more.D", 5000));
            System.out.println("  add(\"more.E\", 50000) -> " + counter.add("more.E", 50000));

            System.out.println("\n2: getMany() - Get the CounterValue/Response object for multiple subjects.\"");
            LinkedBlockingQueue<CounterValueResponse> vResponses = counter.getMany("more.*");
            BigInteger addTotal = BigInteger.ZERO;
            BigInteger multiplyTotal = BigInteger.ONE;
            CounterValueResponse vr = vResponses.poll(1, TimeUnit.SECONDS);
            while (vr != null && vr.isValue()) {
                System.out.println("  " + vr);
                addTotal = addTotal.add(vr.getValue());
                multiplyTotal = multiplyTotal.multiply(vr.getValue());
                vr = vResponses.poll(10, TimeUnit.MILLISECONDS);
            }
            System.out.println("  The iteration is signaled done when the CounterValueResponse is a status: " + vr);
            System.out.println("  Values added totaled: " + addTotal);
            System.out.println("  Values multiplied totaled: " + multiplyTotal);

            System.out.println("\n3: getOrElse() returns the 'else' value if the subject is not found.");
            try {
                counter.get("more.or-else");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"more.or-else\") -> " + e);
            }
            System.out.println("  getOrElse(\"more.or-else\", 77777) -> " + counter.getOrElse("more.or-else", 77777));

            System.out.println("\n4: It's safe to call set() even if the subject did not exist.");
            try {
                counter.get("more.not-found");
            }
            catch (JetStreamApiException e) {
                System.out.println("  get(\"more.not-found\") -> " + e);
            }
            System.out.println("  set(\"more.not-found\", 99999) -> " + counter.add("more.not-found", 99999));
            System.out.println("  get(\"more.not-found\") -> " + counter.get("more.not-found"));
        }
    }
}
