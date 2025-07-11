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
        sb.append(spi).append("servers=").append(r.servers).append(outdent);
        if (r.servers == 1) {
            sb.append(spi).append("serverName=").append(r.serverNamePrefix).append(outdent);
            sb.append(spi).append("port=").append(r.port).append(outdent);
            sb.append(spi).append("listen=").append(r.listen).append(outdent);
            sb.append(spi).append("monitor=").append(r.monitor).append(outdent);
            sb.append(spi).append("url=").append(r.getConnectionUrls()[0]).append(outdent);
        }
        else {
            sb.append(spi).append("clusterName=").append(r.clusterName).append(outdent);
            sb.append(spi).append("serverNamePrefix=").append(r.serverNamePrefix).append(outdent);
            sb.append(spi).append("ports=").append(stringify(r.getConnectionPorts())).append(outdent);
            sb.append(spi).append("listen=").append(stringify(r.getListenPorts())).append(outdent);
            sb.append(spi).append("monitor=").append(stringify(r.getMonitorPorts())).append(outdent);
        }
        sb.append(spi).append("js=").append(r.js).append(outdent);
        if (r.js) {
            sb.append(spi).append("jsStoreDirBase=").append(r.jsStoreDirBase).append(outdent);
        }
        sb.append(spi).append("initialDelay=").append(r.initialDelay).append(outdent);
        sb.append(spi).append("delay=").append(r.delay).append(outdent);
        sb.append(spi).append("downTime=").append(r.downTime).append(outdent);
        sb.append(spi).append("random=").append(r.random);

        return sb.toString();
    }

    private static StringBuilder stringify(int[] ints) {
        StringBuilder sb = new StringBuilder();
        for (int j = 0, intsLength = ints.length; j < intsLength; j++) {
            if (j > 0) {
                sb.append(',');
            }
            sb.append(ints[j]);
        }
        return sb;
    }

    public static void report(String label, Object... parts) {
        String prefix = "[" + System.currentTimeMillis() + "] " + label;
        StringBuilder sb = new StringBuilder(prefix);
        for (Object part : parts) {
            if (part != null) {
                sb.append(" | ");
                sb.append(part);
            }
        }
        System.out.println(sb);
    }
}
