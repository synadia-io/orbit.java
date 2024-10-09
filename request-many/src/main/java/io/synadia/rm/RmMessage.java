// Copyright (c) 2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.rm;

import io.nats.client.Message;
import io.nats.client.impl.StatusMessage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is EXPERIMENTAL, meaning it's api is subject to change.
 * RmMessage is a message given from the RequestMany to the user.
 * This allows us to communicate useful information in addition to or in place of an actual NATS message.
 */
public class RmMessage {
    private static final AtomicLong ID_MAKER = new AtomicLong();

    public static final RmMessage NORMAL_EOD = new RmMessage();

    private final Message message;
    private final Exception exception;
    private final long id;

    RmMessage(Message m) {
        message = m;
        exception = null;
        id = ID_MAKER.incrementAndGet();
    }

    RmMessage(Exception e) {
        message = null;
        exception = e;
        id = ID_MAKER.incrementAndGet();
    }

    private RmMessage() {
        message = null;
        exception = null;
        id = 0;
    }

    /**
     * The internal id of the message, in case the user wants to track.
     * This number is only unique to when this class was statically initialized.
     * @return the id.
     */
    public long getId() {
        return id;
    }

    public Message getMessage() {
        return message;
    }

    public StatusMessage getStatusMessage() {
        return isStatusMessage() ? (StatusMessage)message : null;
    }

    public Exception getException() {
        return exception;
    }

    public boolean isDataMessage() {
        return message != null && !isStatusMessage();
    }

    public boolean isStatusMessage() {
        return message != null && message.isStatusMessage();
    }

    public boolean isException() {
        return exception != null;
    }

    public boolean isEndOfData() {
        return message == null || isStatusMessage() || exception != null;
    }

    public boolean isNormalEndOfData() {
        return message == null && exception == null;
    }

    public boolean isAbnormalEndOfData() {
        return isStatusMessage() || exception != null;
    }

    @Override
    public String toString() {
        if (isEndOfData()) {
            if (isNormalEndOfData()) {
                return "RMM EOD, Normal";
            }
            if (isStatusMessage()){
                return "RMM EOD, Abnormal: " + message;
            }
            return "RMM EOD, Abnormal: " + exception;
        }
        if (message.getData() == null || message.getData().length == 0) {
            return "RMM Data Message: Empty Payload";
        }
        return "RMM Data Message: " + new String(message.getData());
    }
}
