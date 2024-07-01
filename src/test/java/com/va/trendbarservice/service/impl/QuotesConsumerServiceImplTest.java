package com.va.trendbarservice.service.impl;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.va.trendbarservice.model.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import ch.qos.logback.classic.Logger;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Currency;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static com.va.trendbarservice.messages.ExceptionMessages.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@Slf4j
@ExtendWith(MockitoExtension.class)
public class QuotesConsumerServiceImplTest {

    private QuotesConsumerServiceImpl quotesConsumerServiceImpl;

    @Mock
    private ConcurrentLinkedQueue<Quote> quotesQueue;
    @Mock
    private ExecutorService consumerExecutorService;

    private ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;

    private Symbol symbolEURUSD;
    private TrendBarKey keyEURUSD_M1;
    private TrendBarKey keyEURJPY_M1;
    private Quote validTestQuote;
    private Quote validTestQuote2;


    @BeforeEach
    public void setUp() {
        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();
        var symbolEURJPY = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("JPY"))
                .build();
        keyEURUSD_M1 = new TrendBarKey(symbolEURUSD, TrendBarPeriod.M1);
        keyEURJPY_M1 = new TrendBarKey(symbolEURJPY, TrendBarPeriod.M1);

        keyToQuotesQueueMap = new ConcurrentHashMap<>();
        keyToQuotesQueueMap.put(keyEURUSD_M1, new LinkedBlockingQueue<>());
        keyToQuotesQueueMap.put(keyEURJPY_M1, new LinkedBlockingQueue<>());

        var trendBar_EURUSD_M1_Now = new TrendBar(keyEURUSD_M1, Instant.now().truncatedTo(ChronoUnit.MINUTES));
        var trendBar_EURJPY_M1_Now = new TrendBar(keyEURJPY_M1, Instant.now().truncatedTo(ChronoUnit.MINUTES));

        ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap = new ConcurrentHashMap<>();
        currBuildersMap.put(trendBar_EURUSD_M1_Now, Optional.empty());
        currBuildersMap.put(trendBar_EURJPY_M1_Now, Optional.empty());

        validTestQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();
        validTestQuote2 = Quote.builder()
                .symbol(symbolEURJPY)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();

        keyToQuotesQueueMap.put(keyEURUSD_M1, new LinkedBlockingQueue<>());
        keyToQuotesQueueMap.put(keyEURJPY_M1, new LinkedBlockingQueue<>());

        quotesConsumerServiceImpl = QuotesConsumerServiceImpl.builder()
                .quotesQueue(quotesQueue)
                .keyToQuotesQueueMap(keyToQuotesQueueMap)
                .currBuildersMap(currBuildersMap)
                .consumerExecutorService(consumerExecutorService)
                .build();
    }

    @Test
    public void whenStart_thenQuotesQueueWasPolled() {
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            new Thread(() -> {
                runnable.run();
                latch.countDown();
            }).start();
            return null;
        }).when(consumerExecutorService).submit(any(Runnable.class));

        quotesConsumerServiceImpl.start();

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            log.error(e.getMessage());
        }

        verify(quotesQueue, atLeastOnce()).poll();
    }


    @Test
    public void givenValidQuote_whenProcessQuote_thenQuoteAddedToQueue() {
        quotesConsumerServiceImpl.processQuote(validTestQuote);

        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);

        assertNotNull(queue);
        assertTrue(queue.contains(validTestQuote));
    }

    @Test
    public void given2ValidQuotes_whenProcessQuote_thenQuotesAddedTo2Queues() {

        quotesConsumerServiceImpl.processQuote(validTestQuote);
        quotesConsumerServiceImpl.processQuote(validTestQuote2);

        BlockingQueue<Quote> queue1 = keyToQuotesQueueMap.get(keyEURUSD_M1);
        BlockingQueue<Quote> queue2 = keyToQuotesQueueMap.get(keyEURJPY_M1);
        assertNotNull(queue1);
        assertTrue(queue1.contains(validTestQuote));
        assertNotNull(queue2);
        assertTrue(queue2.contains(validTestQuote2));
    }

    @Test
    public void givenNullQuote_whenProcessQuote_thenLogErrorAndReturn() {
        Quote nullQuote = null;

        Logger logger = (Logger) LoggerFactory.getLogger(QuotesConsumerServiceImpl.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        quotesConsumerServiceImpl.processQuote(nullQuote);

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> event.getFormattedMessage().contains(ERROR_QUOTE_IS_NULL)));

        verify(quotesQueue, never()).poll();
        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);
        assertTrue(queue.isEmpty());
    }

    @Test
    public void givenQuoteWithDifferentSymbol_whenProcessQuote_thenQuoteNotAddedToQueue() {
        var differentSymbol = Symbol.builder()
                .baseCurrency(Currency.getInstance("USD"))
                .quoteCurrency(Currency.getInstance("JPY"))
                .build();
        var differentSymbolQuote = Quote.builder()
                .symbol(differentSymbol)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();

        quotesConsumerServiceImpl.processQuote(differentSymbolQuote);
        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);

        assertFalse(queue.contains(differentSymbolQuote));
    }

    @Test
    public void givenQuoteWithNullSymbol_whenProcessQuote_thenQuoteNotAddedToQueue() {
        var differentSymbolQuote = Quote.builder()
                .symbol(null)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();

        Logger logger = (Logger) LoggerFactory.getLogger(QuotesConsumerServiceImpl.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        quotesConsumerServiceImpl.processQuote(differentSymbolQuote);
        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> event.getFormattedMessage().contains(ERROR_QUOTE_SYMBOL_IS_NULL)));

        assertFalse(queue.contains(differentSymbolQuote));
    }

    @Test
    public void givenQuoteWithNullNewPrice_whenProcessQuote_thenQuoteNotAddedToQueue() {
        var nullNewPriceQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(null)
                .unixTimeStamp(Instant.now().toEpochMilli())
                .build();

        Logger logger = (Logger) LoggerFactory.getLogger(QuotesConsumerServiceImpl.class);
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        logger.addAppender(listAppender);

        quotesConsumerServiceImpl.processQuote(nullNewPriceQuote);
        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);

        List<ILoggingEvent> logsList = listAppender.list;
        assertTrue(logsList.stream().anyMatch(event -> event.getFormattedMessage().contains(ERROR_QUOTE_NEW_PRICE_IS_NULL)));

        assertFalse(queue.contains(nullNewPriceQuote));
    }

    @Test
    public void givenQuoteWithNullUnixTimeStamp_whenProcessQuote_thenQuoteNotAddedToQueue() {
        var zeroUnixTimestampQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(0)
                .build();

        quotesConsumerServiceImpl.processQuote(zeroUnixTimestampQuote);
        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);

        assertFalse(queue.contains(zeroUnixTimestampQuote));
    }

    @Test
    public void givenQuoteWithinPeriod_whenProcessQuote_thenQuoteAddedToQueue() {
        long validTimestamp = Instant.now().toEpochMilli();
        var validTimestampQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(validTimestamp)
                .build();

        quotesConsumerServiceImpl.processQuote(validTimestampQuote);

        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);
        assertNotNull(queue);
        assertTrue(queue.contains(validTimestampQuote));
    }

    @Test
    public void givenQuoteOutsidePeriod_whenProcessQuote_thenQuoteNotAddedToQueue() {
        Instant invalidTimestamp = Instant.now().minus(1, ChronoUnit.MINUTES);
        var invalidTimestampQuote = Quote.builder()
                .symbol(symbolEURUSD)
                .newPrice(new BigDecimal("1.1234"))
                .unixTimeStamp(invalidTimestamp.toEpochMilli())
                .build();
        quotesQueue.add(invalidTimestampQuote);

        quotesConsumerServiceImpl.processQuote(invalidTimestampQuote);

        BlockingQueue<Quote> queue = keyToQuotesQueueMap.get(keyEURUSD_M1);
        assertNotNull(queue);
        assertFalse(queue.contains(invalidTimestampQuote));
    }


    @Test
    public void whenShutdown_thenExecutorServiceShutDown() {
        quotesConsumerServiceImpl.shutdown();

        verify(consumerExecutorService).shutdown();
    }
}
