package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.repository.TrendBarRepository;
import com.va.trendbarservice.util.MicroBatcher;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@Slf4j
@ExtendWith(MockitoExtension.class)
public class TrendBarBatchProcessorImplTest {

    private TrendBarBatchProcessorImpl trendBarBatchProcessorImpl;

    @Mock
    private ConcurrentMap<TrendBar, MicroBatcher> currMicroBatchersMap;

    @Mock
    private TrendBarRepository trendBarRepository;

    @Mock
    private ConcurrentMap<TrendBar, TrendBarEntity> currTrendBarEntitiesMap;

    @Mock
    private ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;

    @Mock
    private ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;

    @Mock
    private MicroBatcher microBatcher1;

    @Mock
    private MicroBatcher microBatcher2;

    private TrendBarKey keyEURUSD_M1;
    private Symbol symbolEURUSD;
    private Quote validTestQuote;
    private TrendBar trendBarEURUSD_M1;
    private TrendBarEntity trendBarEntity;


    @BeforeEach
    public void setUp() {

        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);

        trendBarEURUSD_M1 = new TrendBar(keyEURUSD_M1, Instant.now().truncatedTo(ChronoUnit.MINUTES));

        validTestQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();

        trendBarEntity = TrendBarEntity.builder()
                .symbol(symbolEURUSD)
                .openPrice(BigDecimal.ZERO)
                .closePrice(null)
                .highPrice(BigDecimal.ZERO)
                .lowPrice(BigDecimal.TEN)
                .period(TrendBarPeriod.M1)
                .timestamp(System.currentTimeMillis() + 1)
                .status(TrendBarStatus.INCOMPLETE)
                .build();

        trendBarBatchProcessorImpl = Mockito.spy(new TrendBarBatchProcessorImpl(
                currMicroBatchersMap, trendBarRepository, currTrendBarEntitiesMap, keyToQuotesQueueMap, currBuildersMap));
    }

    @Test
    public void givenEmptyQuotesBatch_whenProcessMicroBatch_thenStops() {
        List<Quote> quotesBatch = List.of();

        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBarEURUSD_M1, false);

        verify(currTrendBarEntitiesMap, never()).put(eq(trendBarEURUSD_M1), any(TrendBarEntity.class));
    }

    @Test
    public void givenNotEmptyQuotesBatch_whenProcessMicroBatch_thenContinues() {
        List<Quote> quotesBatch = List.of(validTestQuote);

        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBarEURUSD_M1, false);

        verify(currTrendBarEntitiesMap, atLeastOnce()).put(eq(trendBarEURUSD_M1), any(TrendBarEntity.class));
    }

    @Test
    public void givenNotEmptyQuotesBatch_whenProcessMicroBatch_thenStartsCreateTrendBar() {
        List<Quote> quotesBatch = List.of(validTestQuote);

        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBarEURUSD_M1, false);

        verify(currTrendBarEntitiesMap, atLeastOnce()).put(eq(trendBarEURUSD_M1), any(TrendBarEntity.class));
        verify(trendBarBatchProcessorImpl, atLeastOnce()).createTrendBarEntity(eq(quotesBatch), eq(trendBarEURUSD_M1));
    }

    @Test
    public void givenNotEmptyQuotesBatchFinal_whenProcessMicroBatch_thenStartsUpdateTrendBarAndSaves() {
        List<Quote> quotesBatch = List.of(validTestQuote);
        when(trendBarRepository.save(any())).thenReturn(trendBarEntity);
        when(currTrendBarEntitiesMap.get(any(TrendBar.class))).thenReturn(trendBarEntity);
        when(currMicroBatchersMap.get(any(TrendBar.class))).thenReturn(mock(MicroBatcher.class));

        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBarEURUSD_M1, true);

        verify(trendBarBatchProcessorImpl, atLeastOnce()).updateTrendBarEntity(eq(trendBarEntity), eq(quotesBatch), eq(true));
        verify(trendBarRepository, atLeastOnce()).save(any(TrendBarEntity.class));
    }

    @Test
    public void givenNotEmptyQuotesBatch_whenProcessMicroBatch_thenStartsUpdateTrendBarAndNotSaves() {
        List<Quote> quotesBatch = new ArrayList<>(List.of(validTestQuote));
        int quotesSize = TrendBarBatchProcessorImpl.MAX_BATCH_BUFFER_SIZE - 1;
        for (int i = 0; i < quotesSize; i++) {
            quotesBatch.add(validTestQuote);
        }
        when(currTrendBarEntitiesMap.get(trendBarEURUSD_M1)).thenReturn(trendBarEntity);

        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBarEURUSD_M1, false);

        verify(trendBarBatchProcessorImpl, atLeastOnce()).updateTrendBarEntity(eq(trendBarEntity), eq(quotesBatch), eq(false));
        verify(trendBarRepository, never()).save(any(TrendBarEntity.class));
    }


    @Test
    public void givenNotEmptyQuotesBatch_whenCreateTrendBarEntity_thenReturnsTrendBar() {
        List<Quote> quotesBatch = List.of(validTestQuote);

        var result = trendBarBatchProcessorImpl.createTrendBarEntity(quotesBatch, trendBarEURUSD_M1);

        assertNotNull(result);
        assertEquals(symbolEURUSD, result.getSymbol());
        assertEquals(new BigDecimal("1.1234"), result.getOpenPrice());
        assertEquals(new BigDecimal("1.1234"), result.getHighPrice());
        assertEquals(new BigDecimal("1.1234"), result.getLowPrice());
        assertNull(result.getClosePrice());
        assertEquals(keyEURUSD_M1.trendBarPeriod(), result.getPeriod());
        assertEquals(TrendBarStatus.INCOMPLETE, result.getStatus());
    }

    @Test
    public void givenEmptyQuotesBatch_whenCreateTrendBarEntity_thenReturnsNull() {
        List<Quote> quotesBatch = List.of();

        var result = trendBarBatchProcessorImpl.createTrendBarEntity(quotesBatch, trendBarEURUSD_M1);

        assertNull(result);
    }

    @Test
    public void givenQuotes_whenCreateTrendBarEntity_thenReturnCorrectTrendBarEntity() {
        List<Quote> quotesBatch = new ArrayList<>();
        quotesBatch.add(createQuote(1.2222));
        quotesBatch.add(createQuote(1.1111));
        quotesBatch.add(createQuote(1.3333));
        quotesBatch.add(createQuote(1.4444));

        TrendBarEntity trendBarEntity = trendBarBatchProcessorImpl.createTrendBarEntity(quotesBatch, trendBarEURUSD_M1);

        assertNotNull(trendBarEntity);
        assertEquals(keyEURUSD_M1.symbol(), trendBarEntity.getSymbol());
        assertEquals(keyEURUSD_M1.trendBarPeriod(), trendBarEntity.getPeriod());
        assertEquals(BigDecimal.valueOf(1.2222).setScale(4, RoundingMode.HALF_UP), trendBarEntity.getOpenPrice());
        assertEquals(BigDecimal.valueOf(1.4444).setScale(4, RoundingMode.HALF_UP), trendBarEntity.getHighPrice());
        assertEquals(BigDecimal.valueOf(1.1111).setScale(4, RoundingMode.HALF_UP), trendBarEntity.getLowPrice());
        assertNull(trendBarEntity.getClosePrice());
        assertEquals(TrendBarStatus.INCOMPLETE, trendBarEntity.getStatus());
    }

    @Test
    public void givenEmptyQuotesBatch_whenCreateTrendBarEntity_thenReturnNull() {
        List<Quote> quotesBatch = new ArrayList<>();

        TrendBarEntity trendBarEntity = trendBarBatchProcessorImpl.createTrendBarEntity(quotesBatch, trendBarEURUSD_M1);

        assertNull(trendBarEntity);
    }

    private Quote createQuote(double newPrice) {
        return Quote.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .newPrice(BigDecimal.valueOf(newPrice).setScale(4, RoundingMode.HALF_UP))
                .unixTimeStamp(System.currentTimeMillis())
                .build();
    }


    @Test
    public void givenQuotes_whenUpdateTrendBarEntity_thenReturnUpdatedTrendBarEntity() {
        List<Quote> quotesBatch = new ArrayList<>();
        quotesBatch.add(createQuote(1.3333));
        quotesBatch.add(createQuote(1.4444));
        quotesBatch.add(createQuote(1.2222));
        quotesBatch.add(createQuote(1.1111));

        TrendBarEntity updatedTrendBarEntity = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, false);

        assertNotNull(updatedTrendBarEntity);
        assertEquals(new BigDecimal("1.4444"), updatedTrendBarEntity.getHighPrice());
        assertEquals(new BigDecimal("1.1111"), updatedTrendBarEntity.getLowPrice());
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBarEntity.getStatus());
    }

    @Test
    public void givenEmptyQuotes_whenUpdateTrendBarEntity_thenReturnSameTrendBarEntity() {
        List<Quote> quotesBatch = new ArrayList<>();

        TrendBarEntity updatedTrendBarEntity = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, false);

        assertNotNull(updatedTrendBarEntity);
        assertEquals(trendBarEntity.getHighPrice(), updatedTrendBarEntity.getHighPrice());
        assertEquals(trendBarEntity.getLowPrice(), updatedTrendBarEntity.getLowPrice());
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBarEntity.getStatus());
    }

    @Test
    public void givenQuotesAndBatchFinal_whenUpdateTrendBarEntity_thenReturnFinalizedTrendBarEntity() {
        List<Quote> quotesBatch = new ArrayList<>();
        quotesBatch.add(createQuote(1.1234));
        quotesBatch.add(createQuote(1.1256));
        quotesBatch.add(createQuote(1.1211));
        quotesBatch.add(createQuote(1.1244));

        MicroBatcher mockMicroBatcher = mock(MicroBatcher.class);
        when(currMicroBatchersMap.get(any(TrendBar.class))).thenReturn(mockMicroBatcher);
        doNothing().when(mockMicroBatcher).shutdown();

        TrendBarEntity updatedTrendBarEntity = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, true);

        assertNotNull(updatedTrendBarEntity);
        assertEquals(new BigDecimal("1.1256"), updatedTrendBarEntity.getHighPrice());
        assertEquals(new BigDecimal("1.1211"), updatedTrendBarEntity.getLowPrice());
        assertEquals(new BigDecimal("1.1244"), updatedTrendBarEntity.getClosePrice());
        assertEquals(TrendBarStatus.COMPLETED, updatedTrendBarEntity.getStatus());

        verify(trendBarRepository, times(1)).save(updatedTrendBarEntity);
    }

    @Test
    public void whenDoFinalUpdate_thenReturnFinalizedTrendBarEntity() {
        List<Quote> quotesBatch = new ArrayList<>();
        quotesBatch.add(createQuote(1.1234));
        quotesBatch.add(createQuote(1.1256));
        quotesBatch.add(createQuote(1.1211));
        quotesBatch.add(createQuote(1.1244));

        TrendBarEntity finalizedTrendBarEntity = trendBarBatchProcessorImpl.doFinalUpdate(trendBarEntity, quotesBatch);

        assertNotNull(finalizedTrendBarEntity);
        assertEquals(new BigDecimal("1.1244"), finalizedTrendBarEntity.getClosePrice());
        assertEquals(TrendBarStatus.COMPLETED, finalizedTrendBarEntity.getStatus());
    }

    @Test
    public void givenNotEmptyQuotesBatch_whenUpdateTrendBarAndIsNotBatchFinal_thenUpdatesTrendBar() {
        List<Quote> quotesBatch = new ArrayList<>(List.of(validTestQuote));
        currTrendBarEntitiesMap = new ConcurrentHashMap<>();
        currTrendBarEntitiesMap.put(trendBarEURUSD_M1, trendBarEntity);

        var updatedTrendBar = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, false);

        assertEquals(new BigDecimal("1.1234"), updatedTrendBar.getHighPrice());
        assertEquals(new BigDecimal("1.1234"), updatedTrendBar.getLowPrice());
        assertNull(updatedTrendBar.getClosePrice());
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBar.getStatus());
        verify(trendBarRepository, never()).save(any(TrendBarEntity.class));
    }


    @Test
    public void givenNotEmptyQuotesBatch_AndIsBatchFinalTrue_whenUpdateTrendBarEntity_thenCompletesAndSavesTrendBar() {
        List<Quote> quotesBatch = new ArrayList<>();
        quotesBatch.add(createQuote(1.1234));
        quotesBatch.add(createQuote(1.1256));
        quotesBatch.add(createQuote(1.1211));
        quotesBatch.add(createQuote(1.1244));

        MicroBatcher mockMicroBatcher = mock(MicroBatcher.class);
        when(currMicroBatchersMap.get(any(TrendBar.class))).thenReturn(mockMicroBatcher);
        doNothing().when(mockMicroBatcher).shutdown();

        trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, true);

        verify(trendBarRepository, times(1)).save(any(TrendBarEntity.class));
    }


    @Test
    public void givenQuotesBatchWithLowerPrice_whenUpdateTrendBarEntity_thenUpdatesHighPrice() {
        var lowerPriceQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.0900"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();
        List<Quote> quotesBatch = new ArrayList<>(List.of(lowerPriceQuote));
        currTrendBarEntitiesMap = new ConcurrentHashMap<>();
        currTrendBarEntitiesMap.put(trendBarEURUSD_M1, trendBarEntity);

        var updatedTrendBar = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, false);

        assertEquals(new BigDecimal("1.0900"), updatedTrendBar.getHighPrice());
        assertNull(updatedTrendBar.getClosePrice());
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBar.getStatus());
        verify(trendBarRepository, never()).save(any(TrendBarEntity.class));
    }

    @Test
    public void givenQuotesBatchWithLowerPrice_whenUpdateTrendBarEntity_thenUpdatesLowPrice() {
        var lowerPriceQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("9.0900"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();
        List<Quote> quotesBatch = new ArrayList<>(List.of(lowerPriceQuote));
        currTrendBarEntitiesMap = new ConcurrentHashMap<>();
        currTrendBarEntitiesMap.put(trendBarEURUSD_M1, trendBarEntity);

        var updatedTrendBar = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, false);

        assertEquals(new BigDecimal("9.0900"), updatedTrendBar.getLowPrice());
        assertNull(updatedTrendBar.getClosePrice());
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBar.getStatus());
        verify(trendBarRepository, never()).save(any(TrendBarEntity.class));
    }

    @Test
    public void givenEmptyQuotesBatch_whenUpdateTrendBarEntity_thenDoesNotChangeTrendBar() {
        List<Quote> quotesBatch = new ArrayList<>();
        currTrendBarEntitiesMap = new ConcurrentHashMap<>();
        currTrendBarEntitiesMap.put(trendBarEURUSD_M1, trendBarEntity);

        var updatedTrendBar = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, quotesBatch, false);

        assertEquals(new BigDecimal("0"), updatedTrendBar.getHighPrice());
        assertEquals(new BigDecimal("10"), updatedTrendBar.getLowPrice());
        assertEquals(new BigDecimal("0"), updatedTrendBar.getOpenPrice());
        assertNull(updatedTrendBar.getClosePrice());
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBar.getStatus());
        verify(trendBarRepository, never()).save(any(TrendBarEntity.class));
    }

    @Test
    public void whenShutdownAllMicroBatchers_thenAllMicroBatchersShutdown() {
        when(currMicroBatchersMap.values()).thenReturn(List.of(microBatcher1, microBatcher2));

        trendBarBatchProcessorImpl.shutdownAllMicroBatchers();

        verify(microBatcher1, times(1)).shutdown();
        verify(microBatcher2, times(1)).shutdown();
    }
}
