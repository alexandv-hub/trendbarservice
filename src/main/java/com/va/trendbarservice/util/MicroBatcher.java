package com.va.trendbarservice.util;

import com.va.trendbarservice.model.Quote;
import com.va.trendbarservice.model.TrendBar;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;


@Slf4j
public class MicroBatcher {

    private final LinkedBlockingQueue<Quote> trendBarKeyQuotesQueue;
    private final int executionThreshold;
    private final long timeoutThreshold;
    private final List<Runnable> taskList = new ArrayList<>();
    private final BiConsumer<List<Quote>, Boolean> executionLogic;

    @Getter
    private final ExecutorService executorService;
    @Getter
    private final ScheduledExecutorService scheduledExecutorService;

    @Setter
    private TrendBar trendBar;

    @Getter
    private volatile boolean isBatchFinal;
    private volatile boolean isShuttingDown;

    public MicroBatcher(
            LinkedBlockingQueue<Quote> trendBarKeyQuotesQueue,
            int executionThreshold,
            long timeoutThreshold,
            TrendBar trendBar,
            BiConsumer<List<Quote>, Boolean> executionLogic) {
        this.trendBarKeyQuotesQueue = trendBarKeyQuotesQueue;
        this.executionThreshold = executionThreshold;
        this.timeoutThreshold = timeoutThreshold;
        this.trendBar = trendBar;
        this.executionLogic = executionLogic;
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        startBatchHandling();
    }

    public void submit(Quote quote) {
        trendBarKeyQuotesQueue.add(quote);
    }

    private void startBatchHandling() {
        scheduledExecutorService.scheduleAtFixedRate(this::processBatch, 2000, timeoutThreshold, TimeUnit.MILLISECONDS);
    }

    void processBatch() {
        if (isShuttingDown) {
            log.info("IN MicroBatcher: Shutting down...");
            return;
        }

        List<Quote> quoteList = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        Instant currTrendBarPeriodStart = trendBar.startOfPeriod();
        Instant currTrendbarPeriodEnd = currTrendBarPeriodStart.plus(trendBar.trendBarKey().trendBarPeriod().getDuration());

        isBatchFinal = false;

        try {
            gatherQuotes(quoteList, currTrendbarPeriodEnd, startTime);

            if (!quoteList.isEmpty() || isBatchFinal) {
                processQuotes(quoteList);
            }
        } finally {
            if (isBatchFinal) {
                shutdown();
            }
        }
    }

    void gatherQuotes(List<Quote> quoteList, Instant currTrendbarPeriodEnd, long startTime) {
        while ((System.currentTimeMillis() - startTime) < timeoutThreshold) {
            if (isShuttingDown) {
                break;
            }

            var quotePolled = pollQuote();

            if (quotePolled != null) {
                quoteList.add(quotePolled);
                log.info("IN MicroBatcher: added quote, new quoteList.size() = {}", quoteList.size());

                if (quoteList.size() >= executionThreshold) {
                    log.info("IN MicroBatcher: Batch size threshold reached !!!!!!");
                    break;
                }
            } else {
                if (Instant.now().isAfter(currTrendbarPeriodEnd)) {
                    log.info("IN MicroBatcher: currTrendbarPeriodEnd = {}", currTrendbarPeriodEnd);
                    log.info("IN MicroBatcher: Period ended and queue is empty, setting isBatchFinal = TRUE");
                    isBatchFinal = true;
                    break;
                }
            }
        }
    }

    private Quote pollQuote() {
        try {
            return trendBarKeyQuotesQueue.poll(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void processQuotes(List<Quote> quoteList) {
        final boolean isBatchFinalFinal = isBatchFinal;
        CountDownLatch latch = new CountDownLatch(taskList.size() + 1);

        submitTasks(quoteList, isBatchFinalFinal, latch);

        try {
            latch.await();
            isBatchFinal = false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (quoteList.isEmpty() && isBatchFinal) {
            log.info("IN MicroBatcher: quoteList.isEmpty() && isBatchFinal = TRUE");
            CountDownLatch latch2 = new CountDownLatch(1);

            submitFinalTask(quoteList, latch2);

            try {
                latch2.await();
                isBatchFinal = false;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    void submitTasks(List<Quote> quoteList, boolean isBatchFinalFinal, CountDownLatch latch) {
        Runnable task = () -> {
            try {
                executionLogic.accept(quoteList, isBatchFinalFinal);
            } finally {
                latch.countDown();
            }
        };

        taskList.add(task);
        for (Runnable t : taskList) {
            executorService.submit(t);
        }
        taskList.clear();
    }

    void submitFinalTask(List<Quote> quoteList, CountDownLatch latch2) {
        Runnable task2 = () -> {
            try {
                executionLogic.accept(quoteList, true);
            } finally {
                latch2.countDown();
            }
        };

        taskList.add(task2);
        for (Runnable t : taskList) {
            executorService.submit(t);
        }
        taskList.clear();
    }

    public void shutdown() {
        log.info("IN MicroBatcher: Starting shut down...");
        isShuttingDown = true;

        scheduledExecutorService.shutdown();
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(3, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.error("ExecutorService did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Shutdown complete");
    }
}
