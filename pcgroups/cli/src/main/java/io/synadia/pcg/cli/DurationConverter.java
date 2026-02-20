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

import picocli.CommandLine.ITypeConverter;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Converts human-readable duration strings to Duration objects.
 * Supports formats like: 20ms, 5s, 1m, 2h, 1h30m, 500us, 100ns
 */
public class DurationConverter implements ITypeConverter<Duration> {

    private static final Pattern DURATION_PATTERN = Pattern.compile(
            "(?:(\\d+)h)?(?:(\\d+)m(?!s))?(?:(\\d+)s)?(?:(\\d+)ms)?(?:(\\d+)us)?(?:(\\d+)ns)?",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public Duration convert(String value) throws Exception {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Duration cannot be empty");
        }

        value = value.trim().toLowerCase();

        // Try parsing as a simple number with unit suffix
        Matcher matcher = DURATION_PATTERN.matcher(value);
        if (matcher.matches()) {
            long totalNanos = 0;

            String hours = matcher.group(1);
            String minutes = matcher.group(2);
            String seconds = matcher.group(3);
            String millis = matcher.group(4);
            String micros = matcher.group(5);
            String nanos = matcher.group(6);

            if (hours != null) {
                totalNanos += Long.parseLong(hours) * 3600_000_000_000L;
            }
            if (minutes != null) {
                totalNanos += Long.parseLong(minutes) * 60_000_000_000L;
            }
            if (seconds != null) {
                totalNanos += Long.parseLong(seconds) * 1_000_000_000L;
            }
            if (millis != null) {
                totalNanos += Long.parseLong(millis) * 1_000_000L;
            }
            if (micros != null) {
                totalNanos += Long.parseLong(micros) * 1_000L;
            }
            if (nanos != null) {
                totalNanos += Long.parseLong(nanos);
            }

            if (totalNanos > 0) {
                return Duration.ofNanos(totalNanos);
            }
        }

        // Try single unit formats (e.g., "20ms", "5s")
        Pattern singleUnit = Pattern.compile("(\\d+)(ns|us|ms|s|m|h)", Pattern.CASE_INSENSITIVE);
        Matcher singleMatcher = singleUnit.matcher(value);
        if (singleMatcher.matches()) {
            long amount = Long.parseLong(singleMatcher.group(1));
            String unit = singleMatcher.group(2).toLowerCase();

            switch (unit) {
                case "ns":
                    return Duration.ofNanos(amount);
                case "us":
                    return Duration.ofNanos(amount * 1000);
                case "ms":
                    return Duration.ofMillis(amount);
                case "s":
                    return Duration.ofSeconds(amount);
                case "m":
                    return Duration.ofMinutes(amount);
                case "h":
                    return Duration.ofHours(amount);
            }
        }

        throw new IllegalArgumentException(
                "Invalid duration format: '" + value + "'. Use formats like: 20ms, 5s, 1m, 2h, 1h30m");
    }

    /**
     * Static convenience method for parsing duration strings.
     */
    public static Duration parseDuration(String value) throws Exception {
        return new DurationConverter().convert(value);
    }
}
