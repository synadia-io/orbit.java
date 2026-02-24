// Copyright 2024-2025 Synadia Communications Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.synadia.pcg.cli;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamOptions;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.synadia.pcg.MemberMapping;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * CLI helper utilities.
 */
public class CliUtils {

    private CliUtils() {
        // Utility class
    }

    /**
     * Connects to NATS using the specified context or defaults.
     */
    public static Connection connect(String contextName) throws IOException, InterruptedException {
        Options.Builder builder = Options.builder();

        if (contextName != null && !contextName.isEmpty()) {
            // Try to load context from NATS CLI context file
            NatsContext ctx = loadNatsContext(contextName);
            if (ctx != null) {
                if (ctx.url != null && !ctx.url.isEmpty()) {
                    builder.server(ctx.url);
                }
                if (ctx.user != null && !ctx.user.isEmpty() && ctx.password != null) {
                    builder.userInfo(ctx.user, ctx.password);
                }
                if (ctx.creds != null && !ctx.creds.isEmpty()) {
                    builder.credentialPath(ctx.creds);
                }
                if (ctx.nkey != null && !ctx.nkey.isEmpty()) {
                    builder.authHandler(Nats.staticCredentials(null, ctx.nkey.toCharArray()));
                }
                if (ctx.token != null && !ctx.token.isEmpty()) {
                    builder.token(ctx.token.toCharArray());
                }
            }
        } else {
            // Default connection
            String natsUrl = System.getenv("NATS_URL");
            if (natsUrl == null || natsUrl.isEmpty()) {
                natsUrl = Options.DEFAULT_URL;
            }
            builder.server(natsUrl);
        }

        return Nats.connect(builder.build());
    }

    /**
     * Creates a JetStream context with optional domain.
     */
    public static JetStream getJetStream(Connection nc, String contextName) throws IOException {
        JetStreamOptions.Builder jsoBuilder = JetStreamOptions.builder();

        if (contextName != null && !contextName.isEmpty()) {
            NatsContext ctx = loadNatsContext(contextName);
            if (ctx != null && ctx.jsDomain != null && !ctx.jsDomain.isEmpty()) {
                jsoBuilder.domain(ctx.jsDomain);
            }
        }

        return nc.jetStream(jsoBuilder.build());
    }

    /**
     * Parses member mapping arguments in the format "member:partition1,partition2,...".
     */
    public static List<MemberMapping> parseMemberMappings(List<String> mappingArgs) throws IllegalArgumentException {
        List<MemberMapping> mappings = new ArrayList<>();

        if (mappingArgs == null || mappingArgs.isEmpty()) {
            return mappings;
        }

        for (String mapping : mappingArgs) {
            int colonIndex = mapping.indexOf(':');
            if (colonIndex < 0) {
                throw new IllegalArgumentException("can't parse member mapping '" + mapping + "': missing ':'");
            }

            String memberName = mapping.substring(0, colonIndex);
            String partitionsInput = mapping.substring(colonIndex + 1);
            String[] partitionArgs = partitionsInput.split(",");

            int[] partitions = new int[partitionArgs.length];
            for (int i = 0; i < partitionArgs.length; i++) {
                try {
                    partitions[i] = Integer.parseInt(partitionArgs[i].trim());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("can't parse partition '" + partitionArgs[i] + "': not a valid integer");
                }
            }

            mappings.add(new MemberMapping(memberName, partitions));
        }

        return mappings;
    }

    /**
     * Parses partitioning wildcard indexes from a list of strings.
     */
    public static int[] parsePartitioningWildcards(List<String> wildcardArgs) throws IllegalArgumentException {
        if (wildcardArgs == null || wildcardArgs.isEmpty()) {
            return new int[0];
        }

        int[] wildcards = new int[wildcardArgs.size()];
        for (int i = 0; i < wildcardArgs.size(); i++) {
            try {
                wildcards[i] = Integer.parseInt(wildcardArgs.get(i).trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("can't parse wildcard index '" + wildcardArgs.get(i) + "': not a valid integer");
            }
        }

        return wildcards;
    }

    /**
     * Simple NATS context structure.
     */
    static class NatsContext {
        String url;
        String user;
        String password;
        String creds;
        String nkey;
        String token;
        String jsDomain;
    }

    /**
     * Loads a NATS CLI context from the standard location.
     */
    private static NatsContext loadNatsContext(String contextName) {
        // Try to load from ~/.config/nats/context/<name>.json
        String configDir = System.getenv("XDG_CONFIG_HOME");
        if (configDir == null || configDir.isEmpty()) {
            configDir = System.getProperty("user.home") + "/.config";
        }

        Path contextFile = Paths.get(configDir, "nats", "context", contextName + ".json");
        if (!Files.exists(contextFile)) {
            return null;
        }

        try {
            String content = Files.readString(contextFile);
            return parseContextJson(content);
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * Parses a simple JSON context file.
     */
    private static NatsContext parseContextJson(String json) {
        NatsContext ctx = new NatsContext();

        // Simple JSON parsing without external dependencies
        ctx.url = extractJsonValue(json, "url");
        ctx.user = extractJsonValue(json, "user");
        ctx.password = extractJsonValue(json, "password");
        ctx.creds = extractJsonValue(json, "creds");
        ctx.nkey = extractJsonValue(json, "nkey");
        ctx.token = extractJsonValue(json, "token");
        ctx.jsDomain = extractJsonValue(json, "jetstream_domain");

        return ctx;
    }

    private static String extractJsonValue(String json, String key) {
        String pattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(pattern);
        if (keyIndex < 0) {
            return null;
        }

        int colonIndex = json.indexOf(':', keyIndex + pattern.length());
        if (colonIndex < 0) {
            return null;
        }

        int valueStart = colonIndex + 1;
        while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
            valueStart++;
        }

        if (valueStart >= json.length()) {
            return null;
        }

        if (json.charAt(valueStart) == '"') {
            int valueEnd = json.indexOf('"', valueStart + 1);
            if (valueEnd > valueStart) {
                return json.substring(valueStart + 1, valueEnd);
            }
        }

        return null;
    }

    /**
     * Formats a Duration as a human-readable string (e.g., "20ms", "5s", "1m30s").
     */
    public static String formatDuration(java.time.Duration duration) {
        long totalNanos = duration.toNanos();
        if (totalNanos == 0) return "0s";

        long hours = duration.toHours();
        long minutes = duration.toMinutesPart();
        long seconds = duration.toSecondsPart();
        long millis = duration.toMillisPart();

        StringBuilder sb = new StringBuilder();
        if (hours > 0) sb.append(hours).append("h");
        if (minutes > 0) sb.append(minutes).append("m");
        if (seconds > 0) sb.append(seconds).append("s");
        if (millis > 0 && hours == 0 && minutes == 0 && seconds == 0) sb.append(millis).append("ms");

        return sb.length() > 0 ? sb.toString() : "0s";
    }

    /**
     * Prompts for user confirmation.
     */
    public static boolean confirm(String message) {
        System.out.print(message + " (y/n): ");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String response = reader.readLine();
            return response != null && response.trim().equalsIgnoreCase("y");
        } catch (IOException e) {
            return false;
        }
    }
}
