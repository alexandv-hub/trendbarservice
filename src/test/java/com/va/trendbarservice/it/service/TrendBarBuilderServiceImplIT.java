package com.va.trendbarservice.it.service;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.service.impl.TrendBarBuilderServiceImpl;
import com.va.trendbarservice.util.MicroBatcher;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Currency;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TrendBarBuilderServiceImplIT {

    @Autowired
    private TrendBarBuilderServiceImpl trendBarBuilderServiceImpl;

    @Autowired
    private ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;

    @Autowired
    private ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;

    @Autowired
    private ConcurrentMap<TrendBar, MicroBatcher> currMicroBatchersMap;

    @BeforeEach
    public void setUp() {
        keyToQuotesQueueMap.clear();
        currMicroBatchersMap.clear();
        currBuildersMap.clear();

        trendBarBuilderServiceImpl.initMaps();
    }

    @Test
    public void givenValidSetup_whenInitMaps_thenMapsInitializedCorrectly() {
        assertFalse(keyToQuotesQueueMap.isEmpty());
        assertFalse(currBuildersMap.isEmpty());
        assertEquals(keyToQuotesQueueMap.size(), currBuildersMap.size());
    }

    @Test
    public void givenTrendBar_whenBuildTrendBar_thenTrendBarBuiltAndProcessed() throws InterruptedException {
        var trendBarKey = new TrendBarKey(
                Symbol.builder()
                        .baseCurrency(Currency.getInstance("EUR"))
                        .quoteCurrency(Currency.getInstance("USD"))
                        .build(),
                TrendBarPeriod.M1
        );
        var quote = Quote.builder()
                .symbol(trendBarKey.symbol())
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();
        keyToQuotesQueueMap.get(trendBarKey).add(quote);
        var trendBar = new TrendBar(trendBarKey, Instant.now().truncatedTo(ChronoUnit.MINUTES));

        trendBarBuilderServiceImpl.buildTrendBar(trendBar);
        var microBatcher = currMicroBatchersMap.get(trendBar);

        assertNotNull(microBatcher);
        Thread.sleep(2000);
        assertEquals(0, keyToQuotesQueueMap.get(trendBarKey).size());
    }

    @Test
    public void givenMultipleQuotes_whenBuildTrendBar_thenTrendBarBuiltAndProcessedCorrectly() throws InterruptedException {
        var trendBarKey = new TrendBarKey(
                Symbol.builder()
                        .baseCurrency(Currency.getInstance("EUR"))
                        .quoteCurrency(Currency.getInstance("USD"))
                        .build(),
                TrendBarPeriod.M1
        );
        var quote1 = Quote.builder()
                .symbol(trendBarKey.symbol())
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();
        var quote2 = Quote.builder()
                .symbol(trendBarKey.symbol())
                .newPrice(new BigDecimal("1.1235"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();
        keyToQuotesQueueMap.get(trendBarKey).add(quote1);
        keyToQuotesQueueMap.get(trendBarKey).add(quote2);

        var trendBar = new TrendBar(trendBarKey, Instant.now().truncatedTo(ChronoUnit.MINUTES));
        trendBarBuilderServiceImpl.buildTrendBar(trendBar);

        var microBatcher = currMicroBatchersMap.get(trendBar);
        assertNotNull(microBatcher);
        Thread.sleep(2000);

        assertEquals(0, keyToQuotesQueueMap.get(trendBarKey).size());
    }
}
