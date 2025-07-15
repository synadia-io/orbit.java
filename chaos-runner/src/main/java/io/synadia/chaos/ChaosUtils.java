// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.chaos;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

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

    static ChaosPrinter PRINTER;
    public static ChaosPrinter getDefaultPrinter() {
        if (PRINTER == null) {
            PRINTER = new ChaosPrinter() {
                @Override
                public void out(Object... objects) {
                    if (objects != null && objects.length > 0) {
                        System.out.println(join(objects));
                    }
                }

                @Override
                public void err(Object... objects) {
                    if (objects != null && objects.length > 0) {
                        System.err.println(join(objects));
                    }
                }

                private String join(Object[] objects) {
                    StringBuilder sb = new StringBuilder(TIME_FORMATTER.format(ZonedDateTime.now()));
                    for (Object object : objects) {
                        sb.append(" | ");
                        sb.append(object);
                    }
                    return sb.toString();
                }
            };
        }
        return PRINTER;
    }

    public static final DateTimeFormatter TIME_FORMATTER
        = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static void out(Object... objects) {
        getDefaultPrinter().out(objects);
    }

    public static void err(Object... objects) {
        getDefaultPrinter().err(objects);
    }
}
