package com.va.trendbarservice.util;

import com.va.trendbarservice.model.*;
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
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class MicroBatcherTest {

    private MicroBatcher microBatcher;

    @Mock
    private LinkedBlockingQueue<Quote> trendBarKeyQuotesQueue = new LinkedBlockingQueue<>();
    @Mock
    private BiConsumer<List<Quote>, Boolean> executionLogic;

    private TrendBar trendBar;
    private int executionThreshold;
    private int timeoutThreshold;

    private TrendBarKey keyEURUSD_M1;
    private Symbol symbolEURUSD;

    @BeforeEach
    public void setUp() {
        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);
        Instant startOfPeriod = Instant.now().truncatedTo(ChronoUnit.MINUTES);

        trendBar = new TrendBar(keyEURUSD_M1, startOfPeriod);

        executionThreshold = 5;
        timeoutThreshold = 1000;

        microBatcher = new MicroBatcher(trendBarKeyQuotesQueue, executionThreshold, timeoutThreshold, trendBar, executionLogic);
    }

    @Test
    public void givenQueueWithQuotes_whenProcessBatch_thenProcessed() throws InterruptedException {
        var quote = Quote.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.2345"))
                .unixTimeStamp(System.currentTimeMillis())
                .build();
        when(trendBarKeyQuotesQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(quote).thenReturn(null);
        doNothing().when(executionLogic).accept(anyList(), anyBoolean());

        microBatcher.processBatch();

        verify(trendBarKeyQuotesQueue, atLeastOnce()).poll(anyLong(), any(TimeUnit.class));
        verify(executionLogic, atLeastOnce()).accept(anyList(), anyBoolean());
    }

    @Test
    public void givenEmptyQueueWithPeriodEnded_whenProcessBatch_thenBatchFinal() throws InterruptedException {
        Instant startOfPeriod = Instant.now().truncatedTo(ChronoUnit.DAYS);
        var trendBar = new TrendBar(keyEURUSD_M1, startOfPeriod);
        microBatcher.setTrendBar(trendBar);

        when(trendBarKeyQuotesQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(null);

        microBatcher.processBatch();

        verify(trendBarKeyQuotesQueue, atLeastOnce()).poll(anyLong(), any(TimeUnit.class));
        verify(executionLogic, atLeastOnce()).accept(anyList(), eq(true));
    }

    @Test
    public void givenEmptyQueueWithEndPeriodNotEnded_whenProcessBatch_thenExecutionLogicNotCalled() throws InterruptedException {
        when(trendBarKeyQuotesQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(null);

        microBatcher.processBatch();

        verify(trendBarKeyQuotesQueue, atLeastOnce()).poll(anyLong(), any(TimeUnit.class));
        verifyNoInteractions(executionLogic);
    }

    @Test
    public void givenQuotesReachingThreshold_whenProcessBatch_thenProcessedAndBatchNotFinal() throws InterruptedException {
        var quote = Quote.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.2345"))
                .unixTimeStamp(System.currentTimeMillis())
                .build();
        when(trendBarKeyQuotesQueue.poll(anyLong(), any(TimeUnit.class)))
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(null);
        doNothing().when(executionLogic).accept(anyList(), anyBoolean());

        microBatcher.processBatch();

        verify(trendBarKeyQuotesQueue, atLeastOnce()).poll(anyLong(), any(TimeUnit.class));
        verify(executionLogic, atLeastOnce()).accept(anyList(), eq(false));
    }

    @Test
    public void givenShuttingDown_whenProcessBatch_thenReturnImmediately() {
        microBatcher.shutdown();

        microBatcher.processBatch();

        verifyNoInteractions(trendBarKeyQuotesQueue);
        verifyNoInteractions(executionLogic);
    }


    @Test
    public void givenQueueWithQuotes_whenGatherQuotes_thenQuotesAddedToList() throws InterruptedException {
        List<Quote> quoteList = new ArrayList<>();
        Instant currTrendbarPeriodEnd = Instant.now().plusMillis(2000);
        long startTime = System.currentTimeMillis();

        var quote = Quote.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.2345"))
                .unixTimeStamp(System.currentTimeMillis())
                .build();
        when(trendBarKeyQuotesQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(quote).thenReturn(null);

        microBatcher.gatherQuotes(quoteList, currTrendbarPeriodEnd, startTime);

        verify(trendBarKeyQuotesQueue, atLeastOnce()).poll(anyLong(), any(TimeUnit.class));
        assertFalse(quoteList.isEmpty());
        assertTrue(quoteList.contains(quote));
    }

    @Test
    public void givenEmptyQueueAndPeriodEnd_whenGatherQuotes_thenBatchFinalSetTrue() throws InterruptedException {
        List<Quote> quoteList = new ArrayList<>();
        Instant currTrendbarPeriodEnd = Instant.now();
        long startTime = System.currentTimeMillis();

        when(trendBarKeyQuotesQueue.poll(anyLong(), any(TimeUnit.class))).thenReturn(null);

        microBatcher.gatherQuotes(quoteList, currTrendbarPeriodEnd, startTime);

        verify(trendBarKeyQuotesQueue, atLeastOnce()).poll(anyLong(), any(TimeUnit.class));
        assertTrue(quoteList.isEmpty());
        assertTrue(microBatcher.isBatchFinal());
    }

    @Test
    public void givenQuotesReachingThreshold_whenGatherQuotes_thenStopGathering() throws InterruptedException {
        List<Quote> quoteList = new ArrayList<>();
        Instant currTrendbarPeriodEnd = Instant.now().plusMillis(2000);
        long startTime = System.currentTimeMillis();

        var quote = Quote.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.2345"))
                .unixTimeStamp(System.currentTimeMillis())
                .build();

        when(trendBarKeyQuotesQueue.poll(anyLong(), any(TimeUnit.class)))
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(quote)
                .thenReturn(null);

        microBatcher.gatherQuotes(quoteList, currTrendbarPeriodEnd, startTime);

        verify(trendBarKeyQuotesQueue, atLeast(executionThreshold)).poll(anyLong(), any(TimeUnit.class));
        assertEquals(executionThreshold, quoteList.size());
    }

    @Test
    public void givenShuttingDown_whenGatherQuotes_thenStopGatheringImmediately() {
        List<Quote> quoteList = new ArrayList<>();
        Instant currTrendbarPeriodEnd = Instant.now().plusMillis(2000);
        long startTime = System.currentTimeMillis();

        microBatcher.shutdown();

        microBatcher.gatherQuotes(quoteList, currTrendbarPeriodEnd, startTime);

        verifyNoInteractions(trendBarKeyQuotesQueue);
        assertTrue(quoteList.isEmpty());
    }


    @Test
    public void givenQuoteList_whenSubmitTasks_thenExecutionLogicCalled() throws InterruptedException {
        List<Quote> quoteList = new ArrayList<>();
        boolean isBatchFinalFinal = false;
        CountDownLatch latch = new CountDownLatch(1);
        MicroBatcher localMicroBatcher = new MicroBatcher(trendBarKeyQuotesQueue, executionThreshold, timeoutThreshold, trendBar, executionLogic);

        localMicroBatcher.submitTasks(quoteList, isBatchFinalFinal, latch);
        latch.await(5, TimeUnit.SECONDS);

        verify(executionLogic, times(1)).accept(quoteList, isBatchFinalFinal);
        assertEquals(0, latch.getCount());
    }

    @Test
    public void givenQuoteList_whenSubmitFinalTask_thenExecutionLogicCalled() throws InterruptedException {
        List<Quote> quoteList = new ArrayList<>();
        CountDownLatch latch2 = new CountDownLatch(1);
        MicroBatcher localMicroBatcher = new MicroBatcher(trendBarKeyQuotesQueue, executionThreshold, timeoutThreshold, trendBar, executionLogic);

        localMicroBatcher.submitFinalTask(quoteList, latch2);
        latch2.await(5, TimeUnit.SECONDS);

        verify(executionLogic, times(1)).accept(quoteList, true);
        assertEquals(0, latch2.getCount());
    }
}
