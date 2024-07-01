package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.repository.TrendBarRepository;
import com.va.trendbarservice.service.TrendBarBatchProcessor;
import com.va.trendbarservice.util.MicroBatcher;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

import static com.va.trendbarservice.util.TrendBarUtils.getStartOfPeriod;


@Slf4j
@Service
@Builder
@RequiredArgsConstructor
public class TrendBarBatchProcessorImpl implements TrendBarBatchProcessor {

    public static final int MAX_BATCH_BUFFER_SIZE = 10;

    private final ConcurrentMap<TrendBar, MicroBatcher> currMicroBatchersMap;
    private final TrendBarRepository trendBarRepository;
    private final Map<TrendBar, TrendBarEntity> currTrendBarEntities;
    private final ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;
    private final ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;


    @Override
    public void processMicroBatch(List<Quote> quotesBatch, TrendBar trendBar, boolean isBatchFinal) {
        TrendBarKey trendBarKey = trendBar.trendBarKey();
        String trendBarKeyStr = trendBarKey.symbol() + "_" + trendBarKey.trendBarPeriod();
        log.info("IN processMicroBatch: Processing batch for \"{}\" of size: {}, isBatchFinal = {}", trendBarKeyStr, quotesBatch.size(), isBatchFinal);

        TrendBarEntity currTrendBarEntity = currTrendBarEntities.get(trendBar);

        if (!quotesBatch.isEmpty()) {
            if (currTrendBarEntity == null) {
                var createdTrendBarEntity = createTrendBarEntity(quotesBatch, trendBar);
                log.info("IN processMicroBatch: CREATED new TrendBar {}: currTrendBarEntities.size() = {}", createdTrendBarEntity, currTrendBarEntities.size());
            } else {
                updateTrendBarEntity(currTrendBarEntity, quotesBatch, isBatchFinal);
                log.info("IN processMicroBatch: UPDATED TrendBar {}: currTrendBarEntities.size() = {}", currTrendBarEntity, currTrendBarEntities.size());
            }
        }
    }

        public TrendBarEntity createTrendBarEntity(List<Quote> quotesBatch, TrendBar trendBar) {
        TrendBarKey trendBarKey = trendBar.trendBarKey();
        String trendBarKeyStr = trendBarKey.symbol() + "_" + trendBarKey.trendBarPeriod();
        log.info("Starting createTrendBar \"{}\" for quotesBatch.size() = : {}", trendBarKeyStr, + quotesBatch.size());

        if (!quotesBatch.isEmpty()) {
            Instant firstQuoteUnixTimestamp= Instant.ofEpochMilli(quotesBatch.get(0).getUnixTimeStamp());
            Instant startOfPeriod = getStartOfPeriod(firstQuoteUnixTimestamp, trendBarKey.trendBarPeriod());
            log.info("IN createTrendBar: startOfPeriod = {}", startOfPeriod);

            var openPrice = quotesBatch.get(0).getNewPrice();

            var highPrice = quotesBatch.stream()
                    .map(Quote::getNewPrice)
                    .max(BigDecimal::compareTo)
                    .orElse(BigDecimal.ZERO);
            var lowPrice = quotesBatch.stream()
                    .map(Quote::getNewPrice)
                    .min(BigDecimal::compareTo)
                    .orElse(BigDecimal.ZERO);

            var trendBarEntity = TrendBarEntity.builder()
                    .id(null)
                    .symbol(trendBarKey.symbol())
                    .highPrice(highPrice)
                    .lowPrice(lowPrice)
                    .openPrice(openPrice)
                    .closePrice(null)
                    .timestamp(startOfPeriod.toEpochMilli())
                    .period(trendBarKey.trendBarPeriod())
                    .status(TrendBarStatus.INCOMPLETE)
                    .build();

            currTrendBarEntities.put(trendBar, trendBarEntity);
            return trendBarEntity;
        }
        else {
            log.info("IN createTrendBar: openPrice is empty! Returning null...");
            return null;
        }
    }

//    @Override
    public TrendBarEntity updateTrendBarEntity(TrendBarEntity trendBarEntity, List<Quote> quotesBatch, boolean isBatchFinal) {
        log.info("Starting updateTrendBarEntity: {}", trendBarEntity);
        log.info("quotesBatch.size(): {}", quotesBatch.size());

        TrendBarKey key = new TrendBarKey(trendBarEntity.getSymbol(), trendBarEntity.getPeriod());
        TrendBar trendBar = new TrendBar(key, Instant.ofEpochMilli(trendBarEntity.getTimestamp()));

        if (!quotesBatch.isEmpty()) {
            var highPrice = quotesBatch.stream()
                    .map(Quote::getNewPrice)
                    .max(BigDecimal::compareTo)
                    .orElse(trendBarEntity.getHighPrice());

            var lowPrice = quotesBatch.stream()
                    .map(Quote::getNewPrice)
                    .min(BigDecimal::compareTo)
                    .orElse(trendBarEntity.getLowPrice());

            var highPriceUpdated = highPrice.compareTo(trendBarEntity.getHighPrice()) > 0 ? highPrice : trendBarEntity.getHighPrice();
            var lowPriceUpdated = lowPrice.compareTo(trendBarEntity.getLowPrice()) < 0 ? lowPrice : trendBarEntity.getLowPrice();

            trendBarEntity = trendBarEntity.toBuilder()
                    .highPrice(highPriceUpdated)
                    .lowPrice(lowPriceUpdated)
                    .build();
            currTrendBarEntities.put(trendBar, trendBarEntity);
        }

        if (isBatchFinal) {
            log.info("!!! isBatchFinal = TRUE");

            trendBarEntity = doFinalUpdate(trendBarEntity, quotesBatch);

            currTrendBarEntities.put(trendBar, trendBarEntity);

            trendBarRepository.save(trendBarEntity);
            log.info("Saved trendbar entity: {}", trendBarEntity);

            doCleanMaps(trendBar);
        }

        log.info("Finished update trendBarEntity: {}", trendBarEntity);
        return trendBarEntity;
    }

    public TrendBarEntity doFinalUpdate(TrendBarEntity trendBarEntity, List<Quote> quotesBatch) {
        log.info("Starting doFinalUpdate...");
        var closePrice = quotesBatch.isEmpty() ? trendBarEntity.getOpenPrice() : quotesBatch.get(quotesBatch.size() - 1).getNewPrice();
        return trendBarEntity.toBuilder()
                .closePrice(closePrice)
                .status(TrendBarStatus.COMPLETED)
                .build();
    }

    public void doCleanMaps(TrendBar trendBar) {
        currTrendBarEntities.remove(trendBar);

        currBuildersMap.remove(trendBar);

        currMicroBatchersMap.get(trendBar).shutdown();
        currMicroBatchersMap.remove(trendBar);
    }

    @Override
    public void shutdownAllMicroBatchers() {
        for (var microBatcher : currMicroBatchersMap.values()) {
            microBatcher.shutdown();
        }
        log.info("All microbatchers shut down successfully");
    }
}
