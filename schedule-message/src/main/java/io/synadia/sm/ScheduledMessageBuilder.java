package io.synadia.sm;

import io.nats.client.Message;
import io.nats.client.MessageTtl;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Validator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

/**
 * Class to make setting a per message ttl easier.
 */
public class ScheduledMessageBuilder {

    public enum Predefined {
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

        Predefined(String value) {
            this.value = value;
        }
    }

    private String scheduleString;
    private String publishSubject;
    private String targetSubject;
    private Headers headers;
    private byte[] data;
    private MessageTtl messageTtl;

    public ScheduledMessageBuilder() {
    }

    public ScheduledMessageBuilder(Message message) {
        this.publishSubject = message.getSubject();
        this.headers = message.getHeaders();
        this.data = message.getData();
    }

    /**
     * Set the publish subject
     * @param publishSubject the publish subject
     * @return the builder
     */
    public ScheduledMessageBuilder publishSubject(final String publishSubject) {
        this.publishSubject = publishSubject;
        return this;
    }

    /**
     * Set the target subject
     * @param targetSubject the target subject
     * @return the builder
     */
    public ScheduledMessageBuilder targetSubject(final String targetSubject) {
        this.targetSubject = targetSubject;
        return this;
    }

    /**
     * Set the headers
     * @param headers the headers
     * @return the builder
     */
    public ScheduledMessageBuilder headers(final Headers headers) {
        this.headers = headers;
        return this;
    }

    /**
     * Set the data from a string converting using the
     * charset StandardCharsets.UTF_8
     * @param data the data string
     * @return the builder
     */
    public ScheduledMessageBuilder data(final String data) {
        if (data != null) {
            this.data = data.getBytes(StandardCharsets.UTF_8);
        }
        return this;
    }

    /**
     * Set the data from a string
     * @param data the data string
     * @param charset the charset, for example {@code StandardCharsets.UTF_8}
     * @return the builder
     */
    public ScheduledMessageBuilder data(final String data, final Charset charset) {
        this.data = data.getBytes(charset);
        return this;
    }

    /**
     * Set the data from a byte array. null data changed to empty byte array
     *
     * @param data the data
     * @return the builder
     */
    public ScheduledMessageBuilder data(final byte[] data) {
        this.data = data;
        return this;
    }

    /**
     * Schedule with one of the predefined enum values
     * @param predefined One of the predefined enum values
     * @return a ScheduledMessageBuilder object
     */
    public ScheduledMessageBuilder schedule(Predefined predefined) {
        scheduleString = predefined == null ? null : predefined.value;
        return this;
    }

    /**
     * Schedule with a custom string. Use at your own risk
     * @param custom the time to schedule
     * @return a ScheduledMessageBuilder object
     */
    public ScheduledMessageBuilder scheduleCustom(String custom) {
        scheduleString = custom;
        return this;
    }
    /**
     * Schedule for at a specific time
     * @param zdt the time to schedule
     * @return a ScheduledMessageBuilder object
     */
    public ScheduledMessageBuilder scheduleAt(ZonedDateTime zdt) {
        scheduleString = zdt == null ? null : "@at " + DateTimeUtils.toRfc3339(zdt);
        return this;
    }

    /**
     * Schedule an interval
     * @param every A time specification that complies with Golang's time.ParseDuration() format.
     * @return a ScheduledMessageBuilder object
     */
    public ScheduledMessageBuilder scheduleEvery(String every) {
        every = Validator.emptyAsNull(every);
        if (every == null) {
            scheduleString = null;
        }
        else {
            scheduleString = "@every " + every;
        }
        return this;
    }

    /**
     * Schedule based on standard cron
     * @param cron A valid cron string
     * @return a ScheduledMessageBuilder object
     */
    public ScheduledMessageBuilder scheduleCron(String cron) {
        scheduleString = Validator.emptyAsNull(cron);
        return this;
    }

    public ScheduledMessageBuilder messageTtl(MessageTtl messageTtl) {
        this.messageTtl = messageTtl;
        return this;
    }

    public Message build() {
        Validator.required(publishSubject, "Publish Subject is required.");
        Validator.required(targetSubject, "Target Subject is required.");
        if (Validator.notPrintableOrHasWildGt(publishSubject)) {
            Validator.required(publishSubject, "Publish Subject cannot contain '*' or '>'.");
        }
        if (Validator.notPrintableOrHasWildGt(targetSubject)) {
            Validator.required(targetSubject, "Target Subject cannot contain '*' or '>'.");
        }
        Validator.required(scheduleString, "Schedule is required.");

        if (headers == null) {
            headers = new Headers();
        }
        headers.put("Nats-Schedule-Target", targetSubject);
        headers.put("Nats-Schedule", scheduleString);
        if (messageTtl != null) {
            headers.put("Nats-Schedule-TTL", messageTtl.getTtlString());
        }

        return NatsMessage.builder()
            .subject(publishSubject)
            .headers(headers)
            .data(data)
            .build();
    }
}
