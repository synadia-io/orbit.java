package io.synadia.examples;

import io.nats.client.JetStream;

import static io.synadia.examples.Utils.sleep;

public class ContinuousPublisher implements Runnable {

    private final JetStream js;
    private final String subject;
    private final int pauseMod;
    private final long initialDelay;
    private final long pauseSleep;
    private final long errorSleep;

    private int count;
    private boolean keepGoing;

    public ContinuousPublisher(JetStream js, String subject) {
        this(js, subject, 1000, 100, 1000, 2500);
    }

    public ContinuousPublisher(JetStream js, String subject, long initialDelay, int pauseMod, long pauseSleep, long errorSleep) {
        this.js = js;
        this.subject = subject;
        this.initialDelay = initialDelay;
        this.pauseMod = pauseMod;
        this.pauseSleep = pauseSleep;
        this.errorSleep = errorSleep;
        count = 0;
        keepGoing = true;
    }

    public void stop() {
        keepGoing = false;
    }

    @Override
    public void run() {
        sleep(initialDelay);

        while (keepGoing) {
            try {
                js.publish(subject, null);
                if (++count % pauseMod == 0) {
                    sleep(pauseSleep);
                }
            }
            catch (Exception ignore) {
                sleep(errorSleep);
            }
        }
    }
}
