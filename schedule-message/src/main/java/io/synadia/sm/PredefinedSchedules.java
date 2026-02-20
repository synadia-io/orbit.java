// Copyright (c) 2025-2026 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.sm;

public enum PredefinedSchedules {
    /**
     * Run once a year, midnight, Jan. 1st. Same as Yearly. Equivalent to cron string 0 0 0 1 1 *
     */
    Annually("@annually"),

    /**
     * Run once a year, midnight, Jan. 1st. Same as Annually. Equivalent to cron string 0 0 0 1 1 *
     */
    Yearly("@yearly"),

    /**
     * Run once a month, midnight, first of month. Same as cron format 0 0 0 1 * *
     */
    Monthly("@monthly"),

    /**
     * Run once a week, midnight between Sat/Sun. Equivalent to cron string 0 0 0 * * 0
     */
    Weekly("@weekly"),

    /**
     * Run once a day, midnight. Same as Daily. Equivalent to cron string 0 0 0 * * *
     */
    Midnight("@midnight"),

    /**
     * Run once a day, midnight. Same as Midnight. Equivalent to cron string 0 0 0 * * *
     */
    Daily("@daily"),

    /**
     * Run once an hour, beginning of hour. Equivalent to cron string 0 0 * * * *
     */
    Hourly("@hourly");

    final String value;

    PredefinedSchedules(String value) {
        this.value = value;
    }
}
