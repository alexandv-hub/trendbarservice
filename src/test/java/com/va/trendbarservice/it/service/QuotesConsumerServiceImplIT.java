package com.va.trendbarservice.it.service;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.service.impl.QuotesConsumerServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.Ordered;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Currency;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;


@Slf4j
@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class QuotesConsumerServiceImplIT {

    @Autowired
    private QuotesConsumerServiceImpl quotesConsumerServiceImpl;

    @Autowired
    private ConcurrentLinkedQueue<Quote> quotesQueue;

    @Autowired
    private ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;

    @Autowired
    private ExecutorService consumerExecutorService;

    private Symbol symbolEURUSD;
    private TrendBarKey keyEURUSD_M1;
    private Quote validTestQuote;

    @BeforeEach
    public void setUp() {
        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);

        validTestQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().plus(1, ChronoUnit.MINUTES).toEpochMilli())
                .build();

        keyToQuotesQueueMap.get(keyEURUSD_M1).clear();
        quotesQueue.clear();
    }

    @Test
    public void givenValidQuote_whenStart_thenQueuesWereProcessedCorrectly() throws InterruptedException {
        quotesQueue.add(validTestQuote);

        quotesConsumerServiceImpl.start();
        Thread.sleep(2000);

        assertEquals(0, quotesQueue.size());
        BlockingQueue<Quote> quotesQueueForKey = keyToQuotesQueueMap.get(keyEURUSD_M1);

        assertNotNull(quotesQueueForKey);
        assertTrue(quotesQueueForKey.contains(validTestQuote));
    }

    @Test
    public void givenValidQuote_whenProcessQuote_thenQuoteAddedToQueueMap() {
        quotesQueue.add(validTestQuote);

        quotesConsumerServiceImpl.processQuote(validTestQuote);

        BlockingQueue<Quote> quotesQueueForKey = keyToQuotesQueueMap.get(keyEURUSD_M1);
        assertNotNull(quotesQueueForKey);
        assertTrue(quotesQueueForKey.contains(validTestQuote));
    }

    @Test
    public void givenInvalidQuote_whenProcessQuote_thenQuoteNotAddedToQueueMap() {
            quotesQueue.add(validTestQuote);

            Quote invalidQuote = Quote.builder()
                    .symbol(null)
                    .newPrice(new BigDecimal("1.1234"))
                    .unixTimeStamp(Instant.now().toEpochMilli())
                    .build();

            quotesConsumerServiceImpl.processQuote(invalidQuote);

            BlockingQueue<Quote> quotesQueueForKey = keyToQuotesQueueMap.get(keyEURUSD_M1);
            assertNotNull(quotesQueueForKey);
            assertFalse(quotesQueueForKey.contains(invalidQuote));
    }

    @Test
    public void givenQuoteOutsidePeriod_whenProcessQuote_thenQuoteNotAddedToQueueMap() {
        quotesQueue.add(validTestQuote);

        var quoteOutsidePeriod = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()) // Timestamp outside the period
                .build();

        quotesConsumerServiceImpl.processQuote(quoteOutsidePeriod);

        BlockingQueue<Quote> quotesQueueForKey = keyToQuotesQueueMap.get(keyEURUSD_M1);
        assertNotNull(quotesQueueForKey);
        assertFalse(quotesQueueForKey.contains(quoteOutsidePeriod));
    }

    @Test
    @Order(Ordered.LOWEST_PRECEDENCE)
    public void whenShutdown_thenExecutorServiceTerminated() throws InterruptedException {
        quotesQueue.add(validTestQuote);

        quotesConsumerServiceImpl.start();
        Thread.sleep(1000);
        quotesConsumerServiceImpl.shutdown();

        assertTrue(consumerExecutorService.isShutdown());
        assertTrue(consumerExecutorService.awaitTermination(3, TimeUnit.SECONDS));

        assertEquals(0, quotesQueue.size());
        BlockingQueue<Quote> quotesQueueForKey = keyToQuotesQueueMap.get(keyEURUSD_M1);
        assertNotNull(quotesQueueForKey);
        assertFalse(quotesQueueForKey.isEmpty());
    }
}
