package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.Quote;
import com.va.trendbarservice.model.TrendBar;
import com.va.trendbarservice.model.TrendBarKey;
import com.va.trendbarservice.service.QuotesConsumerService;
import jakarta.annotation.PreDestroy;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.*;

import static com.va.trendbarservice.messages.ExceptionMessages.*;


@Slf4j
@Service
@Builder
@RequiredArgsConstructor
public class QuotesConsumerServiceImpl implements QuotesConsumerService {

    private final ConcurrentLinkedQueue<Quote> quotesQueue;
    private final ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap;
    private final ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap;
    private final ExecutorService consumerExecutorService;

    @Override
    public void start() {
        log.info("Starting QuotesConsumerServiceImpl.start()...");
        consumerExecutorService.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                Quote quote;
                while ((quote = quotesQueue.poll()) != null) {
                    processQuote(quote);
                }
            }
        });
    }

    public void processQuote(Quote quote) {
        if (validateQuote(quote)) return;

        for (var trendBar : currBuildersMap.keySet()) {
            boolean isQuoteTimestampInPeriod = isQuoteTimestampInPeriod(quote, trendBar);
            TrendBarKey trendBarKey = trendBar.trendBarKey();
            if (trendBarKey.symbol().equals(quote.getSymbol()) && isQuoteTimestampInPeriod) {
                BlockingQueue<Quote> quotes = keyToQuotesQueueMap.computeIfAbsent(trendBarKey, k -> new LinkedBlockingQueue<>());

                quotes.add(quote);

                String trendBarKeyStr = trendBarKey.symbol() + "_" + trendBarKey.trendBarPeriod();
                log.info("IN processQuote: ADDED quote to trendBar \"{}\" and NOW keyToQuotesQueueMap.get({}).size() = {}", trendBarKeyStr, trendBarKeyStr, keyToQuotesQueueMap.get(trendBarKey).size());
            }
        }
    }

    private static boolean validateQuote(Quote quote) {
        if (quote == null) {
            log.error(ERROR_QUOTE_IS_NULL);
            return true;
        }
        if (quote.getNewPrice() == null) {
            log.error(ERROR_QUOTE_NEW_PRICE_IS_NULL);
            return true;
        }

        if (quote.getSymbol() == null) {
            log.error(ERROR_QUOTE_SYMBOL_IS_NULL);
            return true;
        }
        return false;
    }

    private boolean isQuoteTimestampInPeriod(Quote quote, TrendBar trendBar) {
        Instant quoteTimestamp = Instant.ofEpochMilli(quote.getUnixTimeStamp());

        Instant startOfPeriod = trendBar.startOfPeriod();

        Instant endOfPeriod = startOfPeriod.plus(trendBar.trendBarKey().trendBarPeriod().getDuration());

        return quoteTimestamp.isAfter(startOfPeriod)
               && quoteTimestamp.isBefore(endOfPeriod);
    }

    @Override
    @PreDestroy
    public void shutdown() {
        log.info("Shutting down quoteConsumerExecutor...");
        consumerExecutorService.shutdown();
        try {
            if (!consumerExecutorService.awaitTermination(3, TimeUnit.SECONDS)) {
                consumerExecutorService.shutdownNow();
                if (!consumerExecutorService.awaitTermination(3, TimeUnit.SECONDS)) {
                    log.error("quoteConsumerExecutor did not terminate");
                }
            }
        } catch (InterruptedException ie) {
            consumerExecutorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("quoteConsumerExecutor shut down successfully");
    }
}
