// Copyright (c) 2025 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.sm;

import io.nats.client.Message;
import io.nats.client.MessageTtl;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.NatsJetStreamConstants;
import io.nats.client.support.Validator;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;

/**
 * Class to make a message that can be published to a stream that allows message scheduling
 */
public class ScheduledMessageBuilder {

    private String scheduleString;
    private String scheduleSubject;
    private String targetSubject;
    private Headers headers;
    private byte[] data;
    private MessageTtl messageTtl;

    public ScheduledMessageBuilder() {}

    /**
     * Set the schedule subject
     * @param scheduleSubject the schedule subject
     * @return the builder
     */
    public ScheduledMessageBuilder scheduleSubject(String scheduleSubject) {
        this.scheduleSubject = scheduleSubject;
        return this;
    }

    /**
     * Set the target subject
     * @param targetSubject the target subject
     * @return the builder
     */
    public ScheduledMessageBuilder targetSubject(String targetSubject) {
        this.targetSubject = targetSubject;
        return this;
    }

    /**
     * Set the data from a byte array. null data changed to empty byte array
     * @param data the data
     * @return the builder
     */
    public ScheduledMessageBuilder data(byte[] data) {
        this.data = data;
        return this;
    }

    /**
     * Set the data from a string converting using the
     * charset StandardCharsets.UTF_8
     * @param data the data string
     * @return the builder
     */
    public ScheduledMessageBuilder data(String data) {
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
    public ScheduledMessageBuilder data(String data, final Charset charset) {
        this.data = data.getBytes(charset);
        return this;
    }

    /**
     * Set the headers
     * @param headers the headers
     * @return the builder
     */
    public ScheduledMessageBuilder headers(Headers headers) {
        this.headers = headers;
        return this;
    }

    /**
     * Copy the subject, data and headers from an existing message
     * @param message the message
     */
    public ScheduledMessageBuilder copy(Message message) {
        scheduleSubject(message.getSubject());
        headers(message.getHeaders());
        return data(message.getData());
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
     * Schedule to run immediately. This is like scheduleAt with time of "now" minus 1 second,
     * which will be in the past by the time it gets to the server,
     * so the scheduled message will be published immediately.
     * @return a ScheduledMessageBuilder object
     */
    public ScheduledMessageBuilder scheduleImmediate() {
        return scheduleAt(DateTimeUtils.gmtNow().minusSeconds(1));
    }

    /**
     * Schedule with one of the predefined enum values
     * @param predefined One of the predefined enum values
     * @return a ScheduledMessageBuilder object
     */
    public ScheduledMessageBuilder schedule(PredefinedSchedules predefined) {
        scheduleString = predefined == null ? null : predefined.value;
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
        Validator.required(scheduleSubject, "Publish Subject is required.");
        Validator.required(targetSubject, "Target Subject is required.");
        if (Validator.notPrintableOrHasWildGt(scheduleSubject)) {
            Validator.required(scheduleSubject, "Publish Subject cannot contain '*' or '>'.");
        }
        if (Validator.notPrintableOrHasWildGt(targetSubject)) {
            Validator.required(targetSubject, "Target Subject cannot contain '*' or '>'.");
        }
        Validator.required(scheduleString, "Schedule is required.");

        if (headers == null) {
            headers = new Headers();
        }
        headers.put(NatsJetStreamConstants.NATS_SCHEDULE_TARGET_HDR, targetSubject);
        headers.put(NatsJetStreamConstants.NATS_SCHEDULE_HDR, scheduleString);
        if (messageTtl != null) {
            headers.put(NatsJetStreamConstants.NATS_SCHEDULE_TTL_HDR, messageTtl.getTtlString());
        }

        return NatsMessage.builder()
            .subject(scheduleSubject)
            .headers(headers)
            .data(data)
            .build();
    }
}
