package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.service.TrendBarBatchProcessor;
import com.va.trendbarservice.service.TrendBarBuilderService;
import com.va.trendbarservice.util.MicroBatcher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static com.va.trendbarservice.util.TrendBarUtils.getStartOfNextPeriod;
import static com.va.trendbarservice.util.TrendBarUtils.getStartOfPeriod;


@Slf4j
@Service
@RequiredArgsConstructor
public class TrendBarBuilderServiceImpl implements TrendBarBuilderService {

    @Setter
    @Value("${microbatcher.execution.threshold.number}")
    private int MICROBATCHER_EXECUTION_THRESHOLD_NUMBER;

    @Setter
    @Value("${microbatcher.timeout.threshold.millis}")
    private long MICROBATCHER_TIMEOUT_THRESHOLD_MILLIS;
    
    private final ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;
    private final ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;
    private final ConcurrentMap<TrendBar, MicroBatcher> currMicroBatchersMap;
    private final TrendBarBatchProcessor batchProcessor;

    @Override
    @PostConstruct
    public void initMaps() {
        log.info("IN initMaps: keyToQuotesQueueMap = {}",keyToQuotesQueueMap.size());
        var symbolList = List.of(
                Symbol.builder()
                        .baseCurrency(Currency.getInstance("EUR"))
                        .quoteCurrency(Currency.getInstance("USD"))
                        .build(),
                Symbol.builder()
                        .baseCurrency(Currency.getInstance("EUR"))
                        .quoteCurrency(Currency.getInstance("JPY"))
                        .build()
        );
        for (var symbol : symbolList) {
            log.info("IN initMaps: symbolList.size() = {}", symbolList.size());
            fillMaps(symbol);
        }
    }

    private void fillMaps(Symbol symbol) {
        TrendBarPeriod[] trendBarPeriods = TrendBarPeriod.values();
        log.info("IN fillMaps: trendBarPeriods.size() = {}", trendBarPeriods.length);
        for (var trendBarPeriod : trendBarPeriods) {

            var key = new TrendBarKey(symbol, trendBarPeriod);
            var trendBar = new TrendBar(key, getStartOfNextPeriod(Instant.now(), trendBarPeriod));
            keyToQuotesQueueMap.put(key, new LinkedBlockingQueue<>());
            currBuildersMap.put(trendBar, Optional.empty());

            log.info("IN fillMaps: keyToQuotesQueueMap = {}", keyToQuotesQueueMap.size());
            log.info("IN fillMaps: keyToQuotesQueueMap.keySet().size() = {}", keyToQuotesQueueMap.keySet().size());
        }
    }

    @Override
    @Transactional()
    public void buildTrendBar(TrendBar trendBar) {
        TrendBarKey key = trendBar.trendBarKey();
        Instant startOfPeriod = getStartOfPeriod(Instant.now(), key.trendBarPeriod());

        String trendBarKeyStr = key.symbol() + "_" + key.trendBarPeriod();
        log.info("Starting build new TrendBar for key: {}... startOfPeriod = {}", trendBarKeyStr, trendBar.startOfPeriod());

        trendBar = new TrendBar(key, startOfPeriod);
        currBuildersMap.put(trendBar, Optional.empty());
        keyToQuotesQueueMap.computeIfAbsent(key, k -> new LinkedBlockingQueue<>());

        var finalTrendBar = trendBar;
        var microBatcher = new MicroBatcher(keyToQuotesQueueMap.get(key), MICROBATCHER_EXECUTION_THRESHOLD_NUMBER, MICROBATCHER_TIMEOUT_THRESHOLD_MILLIS, finalTrendBar, (quotesBatch, isBatchFinal) -> {
            log.info("Processing batch of size: {} for key: {}, isBatchFinal = {}", quotesBatch.size(), trendBarKeyStr, isBatchFinal);
            batchProcessor.processMicroBatch(quotesBatch, finalTrendBar, isBatchFinal);
        });
        currMicroBatchersMap.put(finalTrendBar, microBatcher);

        while (true) {
            var quote = keyToQuotesQueueMap.get(key).poll();
            if (quote != null) {
                currMicroBatchersMap.get(finalTrendBar).submit(quote);
            } else {
                break;
            }
        }
        log.info("FINISHED BUILD TRENDBAR: {} for key: {}", finalTrendBar, trendBarKeyStr);
    }
}
