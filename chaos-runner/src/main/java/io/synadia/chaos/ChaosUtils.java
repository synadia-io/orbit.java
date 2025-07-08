// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.chaos;

public class ChaosUtils {

    public static String toString(ChaosRunner r) {
        return toString(r, System.lineSeparator(), "", "  ", "");
    }

    public static String toString(ChaosRunner r, String sep, String prefix, String indent, String outdent) {
        String spi = sep + prefix + indent;
        StringBuilder sb = new StringBuilder(prefix).append("Chaos Runner:");
        sb.append(spi).append("servers=").append(r.count).append(outdent)
            .append(spi).append("js=").append(r.js).append(outdent);
        if (r.count > 1) {
            sb.append(spi).append("clusterName=").append(r.clusterName).append(outdent)
                .append(spi).append("serverNamePrefix=").append(r.serverNamePrefix).append(outdent);
        }
        sb.append(spi).append("jsStoreDirBase=").append(r.jsStoreDirBase).append(outdent)
            .append(spi).append("initialDelay=").append(r.initialDelay).append(outdent)
            .append(spi).append("delay=").append(r.delay).append(outdent)
            .append(spi).append("downTime=").append(r.downTime).append(outdent)
            .append(spi).append("random=").append(r.random);

        return sb.toString();
    }

    public static void report(String label, Object... parts) {
        String prefix = "[" + System.currentTimeMillis() + "] " + label;
        StringBuilder sb = new StringBuilder(prefix);
        for (Object part : parts) {
            sb.append(" | ");
            sb.append(part);
        }
        System.out.println(sb);
    }

    public static void report(String label, ChaosRunner runner) {
        String prefix =  "[" + System.currentTimeMillis() + "] " + label + " | ";
        System.out.println(toString(runner, System.lineSeparator(), prefix, "  ", ""));
    }
}
