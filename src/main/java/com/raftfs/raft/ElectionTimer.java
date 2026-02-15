package com.raftfs.raft;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class ElectionTimer {

    private static final int MIN_TIMEOUT_MS = 150;
    private static final int MAX_TIMEOUT_MS = 300;

    private final ScheduledExecutorService scheduler;
    private final Runnable onTimeout;
    private volatile ScheduledFuture<?> timerFuture;

    public ElectionTimer(Runnable onTimeout) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "election-timer");
            t.setDaemon(true);
            return t;
        });
        this.onTimeout = onTimeout;
    }

    public void reset() {
        cancel();
        int timeout = ThreadLocalRandom.current().nextInt(MIN_TIMEOUT_MS, MAX_TIMEOUT_MS + 1);
        timerFuture = scheduler.schedule(onTimeout, timeout, TimeUnit.MILLISECONDS);
    }

    public void cancel() {
        ScheduledFuture<?> future = timerFuture;
        if (future != null) {
            future.cancel(false);
        }
    }

    public void shutdown() {
        cancel();
        scheduler.shutdownNow();
    }
}
