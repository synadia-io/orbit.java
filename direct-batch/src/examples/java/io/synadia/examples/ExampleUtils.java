package io.synadia.examples;

import io.nats.client.NUID;
import io.nats.client.api.MessageInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public abstract class ExampleUtils {
    public static void printMessageInfo(LinkedBlockingQueue<MessageInfo> queue) throws InterruptedException {
        printMessageInfo(queueToList(queue));
    }

    public static void printMessageInfo(List<MessageInfo> list) {
        for (int i = 0; i < list.size(); i++) {
            MessageInfo mi = list.get(i);
            if (mi.isMessage()) {
                System.out.println("[MI " + i + "] MI Message"
                    + " | subject: " + mi.getSubject()
                    + " | sequence: " + mi.getSeq());
            }
            else {
                if (mi.isEobStatus()) {
                    System.out.print("[MI " + i + "] EOB");
                }
                else if (mi.isErrorStatus()) {
                    System.out.print("[MI " + i + "] MI Error");
                }
                else if (mi.isErrorStatus()) {
                    System.out.print("[MI " + i + "] MI Status");
                }
                System.out.println(" | isStatus? " + mi.isStatus()
                    + " | isEobStatus? " + mi.isEobStatus()
                    + " | isErrorStatus? " + mi.isErrorStatus()
                    + " | status code: " + mi.getStatus().getCode());
            }
        }
    }

    public static String appendRandomString(String prefix) {
        return prefix + NUID.nextGlobalSequence();
    }

    public static List<MessageInfo> queueToList(LinkedBlockingQueue<MessageInfo> queue) throws InterruptedException {
        List<MessageInfo> list = new ArrayList<>();
        while (true) {
            MessageInfo mi = queue.take();
            list.add(mi);
            if (!mi.isMessage()) {
                break;
            }
        }
        return list;
    }
}
