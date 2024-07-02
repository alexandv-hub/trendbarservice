package com.va.trendbarservice.it.service;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.repository.TrendBarRepository;
import com.va.trendbarservice.service.impl.TrendBarBatchProcessorImpl;
import com.va.trendbarservice.util.MicroBatcher;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;


@Slf4j
@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@ExtendWith(MockitoExtension.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TrendBarBatchProcessorImplIT {

    @Autowired
    private TrendBarBatchProcessorImpl trendBarBatchProcessorImpl;

    @Autowired
    private TrendBarRepository trendBarRepository;

    @MockBean
    private ConcurrentMap<TrendBar, MicroBatcher> currMicroBatchersMap;

    @Autowired
    private ConcurrentMap<TrendBar, TrendBarEntity> currTrendBarEntities;

    @Autowired
    private ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;

    @Autowired
    private ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;

    private Symbol symbolEURUSD;
    private TrendBar trendBar;
    private List<Quote> quotesBatch;

    @BeforeEach
    public void setUp() {
        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        TrendBarKey keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);
        trendBar = new TrendBar(keyEURUSD_M1, Instant.now().truncatedTo(ChronoUnit.MINUTES));
        quotesBatch = List.of(
                Quote.builder()
                        .symbol(symbolEURUSD)
                        .newPrice(new BigDecimal("1.2222"))
                        .unixTimeStamp(Instant.now().plusSeconds(10).toEpochMilli())
                        .build(),
                Quote.builder()
                        .symbol(symbolEURUSD)
                        .newPrice(new BigDecimal("1.1111"))
                        .unixTimeStamp(Instant.now().plusSeconds(10).toEpochMilli())
                        .build(),
                Quote.builder()
                        .symbol(symbolEURUSD)
                        .newPrice(new BigDecimal("1.3333"))
                        .unixTimeStamp(Instant.now().plusSeconds(10).toEpochMilli())
                        .build()
        );

        keyToQuotesQueueMap.get(keyEURUSD_M1).clear();
        currTrendBarEntities.clear();
    }

    @Test
    public void givenValidBatch_whenProcessMicroBatch_thenTrendBarEntityCreated() {

        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBar, false);

        TrendBarEntity trendBarEntity = currTrendBarEntities.get(trendBar);
        assertNotNull(trendBarEntity);
        assertEquals(symbolEURUSD, trendBarEntity.getSymbol());
        assertEquals(new BigDecimal("1.2222"), trendBarEntity.getOpenPrice());
        assertEquals(new BigDecimal("1.3333"), trendBarEntity.getHighPrice());
        assertEquals(new BigDecimal("1.1111"), trendBarEntity.getLowPrice());
        assertNull(trendBarEntity.getClosePrice());
        assertEquals(TrendBarStatus.INCOMPLETE, trendBarEntity.getStatus());
    }

    @Test
    public void givenValidBatchIsBatchFinalTrue_whenProcessMicroBatch_thenTrendBarEntityUpdatedAndSaved() {
        MicroBatcher mockMicroBatcher = mock(MicroBatcher.class);
        when(currMicroBatchersMap.get(any(TrendBar.class))).thenReturn(mockMicroBatcher);
        doNothing().when(mockMicroBatcher).shutdown();

        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBar, false);

        List<Quote> newQuotesBatch = List.of(
                Quote.builder()
                        .symbol(symbolEURUSD)
                        .newPrice(new BigDecimal("1.0000"))
                        .unixTimeStamp(Instant.now().plusSeconds(20).toEpochMilli())
                        .build(),
                Quote.builder()
                        .symbol(symbolEURUSD)
                        .newPrice(new BigDecimal("1.4444"))
                        .unixTimeStamp(Instant.now().plusSeconds(20).toEpochMilli())
                        .build()
        );

        trendBarBatchProcessorImpl.processMicroBatch(newQuotesBatch, trendBar, true);

        Optional<TrendBarEntity> foundTrendBarEntity = trendBarRepository.findById(1L);
        assertNotNull(foundTrendBarEntity);
        assertNotNull(foundTrendBarEntity);
        assertEquals(new BigDecimal("1.2222"), foundTrendBarEntity.get().getOpenPrice());
        assertEquals(new BigDecimal("1.4444"), foundTrendBarEntity.get().getHighPrice());
        assertEquals(new BigDecimal("1.0000"), foundTrendBarEntity.get().getLowPrice());
        assertEquals(new BigDecimal("1.4444"), foundTrendBarEntity.get().getClosePrice());
        assertEquals(TrendBarStatus.COMPLETED, foundTrendBarEntity.get().getStatus());
    }

    @Test
    public void givenValidBatchIsBatchFinalTrue_whenUpdateTrendBarEntity_thenTrendBarEntityUpdated() {
        trendBarBatchProcessorImpl.processMicroBatch(quotesBatch, trendBar, false);
        List<Quote> newQuotesBatch = List.of(
                Quote.builder()
                        .symbol(symbolEURUSD)
                        .newPrice(new BigDecimal("1.0000"))
                        .unixTimeStamp(Instant.now().plusSeconds(20).toEpochMilli())
                        .build(),
                Quote.builder()
                        .symbol(symbolEURUSD)
                        .newPrice(new BigDecimal("1.4444"))
                        .unixTimeStamp(Instant.now().plusSeconds(20).toEpochMilli())
                        .build()
        );
        TrendBarEntity trendBarEntity = currTrendBarEntities.get(trendBar);

        TrendBarEntity updatedTrendBarEntity = trendBarBatchProcessorImpl.updateTrendBarEntity(trendBarEntity, newQuotesBatch, false);

        assertNotNull(updatedTrendBarEntity);
        assertEquals(new BigDecimal("1.2222"), updatedTrendBarEntity.getOpenPrice());
        assertEquals(new BigDecimal("1.4444"), updatedTrendBarEntity.getHighPrice());
        assertEquals(new BigDecimal("1.0000"), updatedTrendBarEntity.getLowPrice());
        assertNull(updatedTrendBarEntity.getClosePrice());
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBarEntity.getStatus());
    }

    @Test
    public void givenEmptyBatch_whenProcessMicroBatch_thenTrendBarEntityNotCreated() {
        trendBarBatchProcessorImpl.processMicroBatch(Collections.emptyList(), trendBar, false);

        TrendBarEntity trendBarEntity = currTrendBarEntities.get(trendBar);
        assertNull(trendBarEntity);
    }

    @Test
    public void givenEmptyBatch_whenUpdateTrendBarEntity_thenStatusNotUpdated() {
        TrendBarEntity trendBarEntity = trendBarBatchProcessorImpl.createTrendBarEntity(quotesBatch, trendBar);
        currTrendBarEntities.put(trendBar, trendBarEntity);

        trendBarBatchProcessorImpl.processMicroBatch(Collections.emptyList(), trendBar, true);

        TrendBarEntity updatedTrendBarEntity = currTrendBarEntities.get(trendBar);
        assertNotNull(updatedTrendBarEntity);
        assertEquals(TrendBarStatus.INCOMPLETE, updatedTrendBarEntity.getStatus());
    }

    @Test
    public void givenValidBatch_whenDoFinalUpdate_thenTrendBarEntityCompleted() {
        TrendBarEntity trendBarEntity = trendBarBatchProcessorImpl.createTrendBarEntity(quotesBatch, trendBar);
        currTrendBarEntities.put(trendBar, trendBarEntity);

        trendBarEntity = trendBarBatchProcessorImpl.doFinalUpdate(trendBarEntity, quotesBatch);

        assertEquals(TrendBarStatus.COMPLETED, trendBarEntity.getStatus());
        assertEquals(quotesBatch.get(quotesBatch.size() - 1).getNewPrice(), trendBarEntity.getClosePrice());
    }

}
