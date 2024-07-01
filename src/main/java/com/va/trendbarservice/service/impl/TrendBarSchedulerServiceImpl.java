package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.TrendBar;
import com.va.trendbarservice.service.QuotesConsumerService;
import com.va.trendbarservice.service.TrendBarBatchProcessor;
import com.va.trendbarservice.service.TrendBarBuilderService;
import com.va.trendbarservice.service.TrendBarSchedulerService;
import jakarta.annotation.PreDestroy;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static com.va.trendbarservice.util.TrendBarUtils.getInitialDelayInMillis;
import static com.va.trendbarservice.util.TrendBarUtils.getStartOfNextPeriod;


@Slf4j
@Setter
@Service
@Builder
@RequiredArgsConstructor
public class TrendBarSchedulerServiceImpl implements TrendBarSchedulerService {

    private final QuotesConsumerService quotesConsumerService;
    private final ExecutorService consumerExecutorService;
    private final ScheduledExecutorService scheduler;

    private final ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;
    private final TrendBarBatchProcessor batchProcessor;
    private final TrendBarBuilderService trendBarBuilderService;

    @Override
    public void startQuotesConsumer() {
        consumerExecutorService.submit(quotesConsumerService::start);
    }

    @Override
    public void startAllTrendBarBuildersWithInitialDelays() {
        for (var trendBar : currBuildersMap.keySet()) {
            var trendBarKey = trendBar.trendBarKey();
            long currTrendBarKeyInitialDelayInMillis = getInitialDelayInMillis(trendBarKey.trendBarPeriod());

            Instant startOfNextPeriod = getStartOfNextPeriod(Instant.now(), trendBarKey.trendBarPeriod());
            trendBar = trendBar.toBuilder().startOfPeriod(startOfNextPeriod).build();

            String trendBarKeyStr = trendBarKey.symbol() + "_" + trendBarKey.trendBarPeriod();
            log.info("startOfNextPeriod for TrendBarKey {} was added with value =  {}", trendBarKeyStr, startOfNextPeriod);

            startScheduledTrendBarBuild(trendBar, currTrendBarKeyInitialDelayInMillis);
        }
    }

    @Override
    public void startScheduledTrendBarBuild(TrendBar trendBar, long initialDelayInMillis) {
        var trendBarKey = trendBar.trendBarKey();
        String trendBarKeyStr = trendBarKey.symbol() + "_" + trendBarKey.trendBarPeriod();
        log.info("Starting schedule new trend bar build for key = {}", trendBarKeyStr);

        currBuildersMap.computeIfPresent(trendBar, (key, scheduledFutureOptional) -> {
            log.info(">>>> ");
            if (scheduledFutureOptional.isEmpty()) {
                long periodInMillis = trendBarKey.trendBarPeriod().getDuration().toMillis();

                ScheduledFuture<?> scheduledFuture = scheduler.scheduleAtFixedRate(() ->
                        trendBarBuilderService.buildTrendBar(trendBar), initialDelayInMillis, periodInMillis, TimeUnit.MILLISECONDS);

                log.info("Successfully scheduled new trend bar build for key {} WITH INITIAL DELAY = {}", trendBarKeyStr, initialDelayInMillis);
                return Optional.of(scheduledFuture);
            } else {
                log.warn("TrendBarKey already has a scheduled task for this trendBarKey: {}", trendBarKeyStr);
                return scheduledFutureOptional;
            }
        });
    }

    @Override
    @PreDestroy
    public void shutDown() {
        log.info("Shutting down scheduler and microbatchers...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    log.error("Scheduler did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("Scheduler shut down successfully");

        batchProcessor.shutdownAllMicroBatchers();
    }
}
