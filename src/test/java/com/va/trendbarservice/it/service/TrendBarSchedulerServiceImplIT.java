package com.va.trendbarservice.it.service;


import com.va.trendbarservice.model.*;
import com.va.trendbarservice.service.impl.TrendBarSchedulerServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.util.Currency;
import java.util.Optional;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
public class TrendBarSchedulerServiceImplIT {

    @Autowired
    private TrendBarSchedulerServiceImpl trendBarSchedulerServiceImpl;

    @Autowired
    private ScheduledExecutorService scheduler;

    @Autowired
    private ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;

    private TrendBarKey keyEURUSD_M1;

    @BeforeEach
    public void setUp() {
        Symbol symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);
    }

    @Test
    public void givenValidSetup_whenStartQuotesConsumer_thenQuotesConsumerStarted() {
        assertDoesNotThrow(() -> trendBarSchedulerServiceImpl.startQuotesConsumer());
    }

    @Test
    public void givenTrendBar_whenStartScheduledTrendBarBuild_thenScheduledCorrectly() {
        var trendBar = new TrendBar(keyEURUSD_M1, Instant.now());
        currBuildersMap.put(trendBar, Optional.empty());

        trendBarSchedulerServiceImpl.startScheduledTrendBarBuild(trendBar, 1000L);

        assertTrue(currBuildersMap.get(trendBar).isPresent());
    }

    @Test
    public void givenTrendBarSchedulerServiceImpl_whenShutdown_thenServicesStoppedCorrectly() {
        trendBarSchedulerServiceImpl.shutDown();
        assertTrue(scheduler.isShutdown());
    }
}
