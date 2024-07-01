package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.service.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Currency;
import java.util.Optional;
import java.util.concurrent.*;

import static com.va.trendbarservice.util.TrendBarUtils.getStartOfNextPeriod;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;


@Slf4j
@ExtendWith(MockitoExtension.class)
public class TrendBarSchedulerServiceImplTest {

    private TrendBarSchedulerServiceImpl trendBarSchedulerServiceImpl;

    @Mock
    private QuotesConsumerService quotesConsumerService;

    @Mock
    private ExecutorService consumerExecutorService;

    @Mock
    private ScheduledExecutorService scheduler;

    @Mock
    private ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;

    @Mock
    private TrendBarBatchProcessor batchProcessor;

    @Mock
    private TrendBarBuilderService trendBarBuilderService;


    private TrendBar trendBarEURUSD_M1;

    @BeforeEach
    public void setUp() {

        Symbol symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        var keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);
        var keyEURUSD_H1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.H1);
        var keyEURUSD_D1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.D1);

        Instant now = Instant.now();
        trendBarEURUSD_M1 = new TrendBar(keyEURUSD_M1, getStartOfNextPeriod(now, keyEURUSD_D1.trendBarPeriod()));
        var trendBarEURUSD_H1 = new TrendBar(keyEURUSD_H1, getStartOfNextPeriod(now, keyEURUSD_D1.trendBarPeriod()));
        var trendBarEURUSD_D1 = new TrendBar(keyEURUSD_D1, getStartOfNextPeriod(now, keyEURUSD_D1.trendBarPeriod()));

        currBuildersMap = new ConcurrentHashMap<>();
        currBuildersMap.put(trendBarEURUSD_M1, Optional.empty());
        currBuildersMap.put(trendBarEURUSD_H1, Optional.empty());
        currBuildersMap.put(trendBarEURUSD_D1, Optional.empty());

        trendBarSchedulerServiceImpl = TrendBarSchedulerServiceImpl.builder()
                .quotesConsumerService(quotesConsumerService)
                .consumerExecutorService(consumerExecutorService)
                .scheduler(scheduler)
                .currBuildersMap(currBuildersMap)
                .batchProcessor(batchProcessor)
                .trendBarBuilderService(trendBarBuilderService)
                .build();
    }

    @AfterEach
    public void tearDown() {
        trendBarSchedulerServiceImpl.shutDown();
    }


    @Test
    public void whenStartQuotesConsumer_thenConsumerExecutorWasCalled() {
        trendBarSchedulerServiceImpl.startQuotesConsumer();

        verify(consumerExecutorService).submit(any(Runnable.class));
    }

    @Test
    public void givenM1TrendBarPeriod_whenStartScheduledTrendBarBuild_AndNoExistingTask_thenScheduleNewTask2() {
        long initialDelayInMillis = 1000L;
        long periodInMillis = TrendBarPeriod.M1.getDuration().toMillis();

        when(scheduler.scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInMillis), eq(periodInMillis), eq(TimeUnit.MILLISECONDS)))
                .thenReturn(mock(ScheduledFuture.class));

        trendBarSchedulerServiceImpl.startScheduledTrendBarBuild(trendBarEURUSD_M1, initialDelayInMillis);

        verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInMillis), eq(periodInMillis), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void whenShutdown_thenSchedulerShutDown() throws InterruptedException {
        trendBarSchedulerServiceImpl.shutDown();
        Thread.sleep(3000);

        verify(scheduler).shutdown();
        verify(batchProcessor).shutdownAllMicroBatchers();
    }
}
