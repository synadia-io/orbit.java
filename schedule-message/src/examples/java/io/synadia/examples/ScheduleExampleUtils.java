// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Message;
import io.nats.client.impl.Headers;

public class ScheduleExampleUtils {

    public static void report(Object... objects) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (Object o : objects) {
            if (first) {
                first = false;
            }
            else {
                sb.append(" | ");
            }
            if (o instanceof Message) {
                sb.append(toString((Message)o));
            }
            else {
                sb.append(o.toString());
            }
        }
        System.out.println("[" + System.currentTimeMillis() + "] " + sb);
    }

    public static String toString(Message msg) {
        StringBuilder sb = new StringBuilder(System.lineSeparator())
            .append("  Subject: ").append(msg.getSubject());
        if (msg.getData() == null || msg.getData().length == 0) {
            sb.append(" | No Data");
        }
        else {
            sb.append(" | Data: ").append(new String(msg.getData()));
        }
        Headers h = msg.getHeaders();
        if (h != null && !h.isEmpty()) {
            sb.append(System.lineSeparator()).append("  Headers:");
            for (String key : h.keySet()) {
                sb.append(System.lineSeparator()).append("    ");
                sb.append(key).append("=").append(h.get(key));
            }
        }
        return sb.toString();
    }
}
