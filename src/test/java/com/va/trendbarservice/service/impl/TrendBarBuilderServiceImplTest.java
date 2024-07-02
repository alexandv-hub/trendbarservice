package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.service.TrendBarBatchProcessor;
import com.va.trendbarservice.util.MicroBatcher;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class TrendBarBuilderServiceImplTest {

    private TrendBarBuilderServiceImpl trendBarBuilderServiceImpl;

    @Mock
    private ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;

    @Mock
    private ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;

    @Mock
    private TrendBarBatchProcessor batchProcessor;

    @Mock
    private ConcurrentMap<TrendBar, MicroBatcher> currMicroBatchersMap;

    @Mock
    private MicroBatcher microBatcher;


    private TrendBarKey keyEURUSD_M1;
    private LinkedBlockingQueue<Quote> quoteQueue;
    private Symbol symbolEURUSD;
    private TrendBar trendBarEURUSD_M1;

    @BeforeEach
    public void setUp() {
        keyToQuotesQueueMap = new ConcurrentHashMap<>();
        currBuildersMap = new ConcurrentHashMap<>();

        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);
        trendBarEURUSD_M1 = new TrendBar(keyEURUSD_M1, Instant.now().truncatedTo(ChronoUnit.MINUTES));

        currBuildersMap = new ConcurrentHashMap<>();
        currBuildersMap.put(trendBarEURUSD_M1, Optional.empty());

        trendBarBuilderServiceImpl = new TrendBarBuilderServiceImpl(
                keyToQuotesQueueMap,
                currBuildersMap,
                currMicroBatchersMap,
                batchProcessor
        );
        trendBarBuilderServiceImpl.setMICROBATCHER_EXECUTION_THRESHOLD_NUMBER(10);
        trendBarBuilderServiceImpl.setMICROBATCHER_TIMEOUT_THRESHOLD_MILLIS(10_000);
    }

    @Test
    public void whenInitMaps_thenMapsNotEmpty() {
        log.info("keyToQuotesQueueMap.keySet(): {}", keyToQuotesQueueMap.keySet());
        log.info("keyToQuotesQueueMap.get(keyEURUSD_M1): {}", keyToQuotesQueueMap.get(keyEURUSD_M1));
        log.info("keyToQuotesQueueMap size BEFORE INIT: {}",  keyToQuotesQueueMap.size());

        trendBarBuilderServiceImpl.initMaps();

        assertFalse(keyToQuotesQueueMap.isEmpty(), "keyToQuotesQueueMap should not be empty after initialization");
        log.info("keyToQuotesQueueMap size AFTER INIT: {}", keyToQuotesQueueMap.size());
        log.info("keyToQuotesQueueMap.get(keyEURUSD_M1): {}", keyToQuotesQueueMap.get(keyEURUSD_M1));
        log.info("keyToQuotesQueueMap size: {}", keyToQuotesQueueMap.size());

        assertFalse(currBuildersMap.isEmpty(), "currBuildersMap should not be empty after initialization");
        log.info("currBuildersMap size: {}", currBuildersMap.size());
    }


    @Test
    public void whenBuildTrendBars_thenMicroBatcherInitializedAndQuoteProcessed() {
        trendBarBuilderServiceImpl.initMaps();
        quoteQueue = new LinkedBlockingQueue<>();
        when(currMicroBatchersMap.get(trendBarEURUSD_M1)).thenReturn(microBatcher);

        long unixTimeStamp = System.currentTimeMillis();
        var quote = Quote.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(unixTimeStamp)
                .build();
        quoteQueue.add(quote);

        keyToQuotesQueueMap.put(keyEURUSD_M1, quoteQueue);

        trendBarBuilderServiceImpl.buildTrendBar(trendBarEURUSD_M1);

        assertNotNull(currMicroBatchersMap.get(trendBarEURUSD_M1));
        verify(microBatcher, times(1)).submit(eq(quote));
    }

    @Test
    public void whenBuildTrendBarsAndQueueIsEmpty_thenNoProcessing() {
        quoteQueue = new LinkedBlockingQueue<>();
        when(currMicroBatchersMap.get(trendBarEURUSD_M1)).thenReturn(microBatcher);

        long unixTimeStamp = System.currentTimeMillis();
        var quote = Quote.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(unixTimeStamp)
                .build();

        keyToQuotesQueueMap.put(keyEURUSD_M1, quoteQueue);

        trendBarBuilderServiceImpl.buildTrendBar(trendBarEURUSD_M1);

        assertNotNull(currMicroBatchersMap.get(trendBarEURUSD_M1));
        verify(microBatcher, never()).submit(eq(quote));
        verify(batchProcessor, never()).processMicroBatch(anyList(), eq(trendBarEURUSD_M1), anyBoolean());
    }
}
