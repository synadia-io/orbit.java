package io.nats.client.impl;

import io.nats.client.Message;
import io.nats.client.support.IncomingHeadersProcessor;

public class ImplUtils {

    public static Message createStatusMessage() {
        IncomingHeadersProcessor incomingHeadersProcessor =
            new IncomingHeadersProcessor("NATS/1.0 503 No Responders\r\n".getBytes());
        IncomingMessageFactory factory =
            new IncomingMessageFactory("sid", "subj", "replyTo", 0, false);
        factory.setHeaders(incomingHeadersProcessor);
        factory.setData(null); // coverage
        return factory.getMessage();
    }
}
