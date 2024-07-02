package com.va.trendbarservice.it;

import com.va.trendbarservice.model.Quote;
import com.va.trendbarservice.model.Symbol;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;


@Slf4j
@Component
@RequiredArgsConstructor
public class QuotesProducerTestService {

    @Setter
    @Value("${quotesProducerTestService.interval.millis}")
    private long INTERVAL;

    @Autowired
    public final ConcurrentLinkedQueue<Quote> quotesQueue;

    private final Random random = new Random();

    @Autowired
    private ScheduledExecutorService scheduledExecutorService;

    @PostConstruct
    public void init() {
        scheduledExecutorService = Executors.newScheduledThreadPool(1);
    }

    @PreDestroy
    public void shutdown() {
        scheduledExecutorService.shutdown();
    }

    public void start() {
        log.info("Starting QuotesProducerService.generateQuotes()...");

        var symbols = List.of(
                Symbol.builder()
                        .baseCurrency(Currency.getInstance("EUR"))
                        .quoteCurrency(Currency.getInstance("USD"))
                        .build(),
                Symbol.builder()
                        .baseCurrency(Currency.getInstance("EUR"))
                        .quoteCurrency(Currency.getInstance("JPY"))
                        .build()
        );

        for (var symbol : symbols) {
            var runnable = generateQuote(symbol);
            // Расчет интервала времени: 60,000 миллисекунд / 10,000 котировок = 6 миллисекунд на котировку
            // Расчет интервала времени: 60,000 миллисекунд / 1,000 котировок = 60 миллисекунд на котировку
            long interval = INTERVAL; // интервал в миллисекундах

            scheduledExecutorService.scheduleAtFixedRate(runnable, 0, interval, TimeUnit.MILLISECONDS);
        }
    }

    public Runnable generateQuote(Symbol symbol) {
        return () -> {
            var newRandomPrice = BigDecimal.valueOf(1 + (100 * random.nextDouble()));
            long unixTimeStamp = System.currentTimeMillis();

            var quote = Quote.builder()
                    .id(null)
                    .symbol(symbol)
                    .newPrice(newRandomPrice)
                    .unixTimeStamp(unixTimeStamp)
                    .build();
            quotesQueue.offer(quote);
        };
    }
}
