// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.examples;

import io.nats.client.Message;
import io.nats.client.impl.Headers;

/**
 * Small console-logging helpers shared by the example apps.
 */
public class ScheduleExampleUtils {

    private ScheduleExampleUtils() {}

    /**
     * Print a pipe-separated, timestamped line to {@code System.out}. Each object is
     * rendered with {@link Object#toString()}, except {@link Message}, which is rendered
     * via {@link #toString(Message)}.
     * @param objects the values to render
     */
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

    /**
     * Format a {@link Message} as a short multi-line string showing the subject, data
     * (when present), and headers (when any).
     * @param msg the message to format
     * @return a human-readable representation
     */
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
