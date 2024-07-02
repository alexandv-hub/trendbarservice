package com.va.trendbarservice.it.demo;

import com.va.trendbarservice.it.QuotesProducerTestService;
import com.va.trendbarservice.model.*;
import com.va.trendbarservice.repository.TrendBarRepository;
import com.va.trendbarservice.service.TrendBarHistoryService;
import com.va.trendbarservice.service.TrendBarStarterService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Currency;
import java.util.List;
import java.util.concurrent.*;

import static com.va.trendbarservice.util.TrendBarUtils.getInitialDelayInMillis;
import static org.junit.jupiter.api.Assertions.*;


@Slf4j
@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TrendBarEntityBuilderServiceImplFullDemoIT {

    @Autowired
    private QuotesProducerTestService quotesProducerTestService;

    @Autowired
    private TrendBarStarterService trendBarStarter;

    @Autowired
    private TrendBarHistoryService trendBarHistoryService;

    @Autowired
    private TrendBarRepository trendBarRepository;

    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;

    private Symbol symbolEURUSD;
    private Symbol symbolEURJPY;

    @BeforeEach
    public void setUp() {
        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();
        symbolEURJPY = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("JPY"))
                .build();

        executorService = Executors.newFixedThreadPool(4);
        scheduledExecutorService = Executors.newScheduledThreadPool(4);
    }

    @AfterEach
    public void tearDown() {
        System.out.println("Shutting down !!!!!!");
        shutdownExecutorService(executorService);
        shutdownExecutorService(scheduledExecutorService);
    }

    private void shutdownExecutorService(ExecutorService executorService) {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void testCanBuildTrendBarsAnReadFromDataBase() throws InterruptedException {
        Thread t1 = new Thread("quotesProducer") {
            public void run() {
                quotesProducerTestService.start();
            }
        };
        Thread t2 = new Thread("trendBarStarter") {
            public void run() {
                trendBarStarter.start();
            }
        };
        executorService.execute(t1);
        executorService.execute(t2);


        Runnable task1 = () -> {
            try {
                log.info("IN task1 - starting...");
                List<TrendBarEntity> trendBarsInRange = trendBarHistoryService.getTrendBarsBySymbolAndPeriodAndTimestampInRange(
                        symbolEURUSD,
                        TrendBarPeriod.M1,
                        Instant.now().minus(100, ChronoUnit.SECONDS).toEpochMilli(),
                        Instant.now().toEpochMilli()
                );
                log.info("IN task1 TrendBarsInRange size: {}", trendBarsInRange.size());
            } catch (Exception e) {
                log.error("Exception in task1", e);
            }
        };
        Runnable task2 = () -> {
            try {
                log.info("IN task2 - starting...");
                List<TrendBarEntity> trendBarsFrom = trendBarHistoryService.getTrendBarsBySymbolAndPeriodFrom(
                        symbolEURJPY,
                        TrendBarPeriod.M1,
                        Instant.now().minus(100, ChronoUnit.SECONDS).toEpochMilli()
                );
                log.info("IN task2 TrendBarsFrom size: {}", trendBarsFrom.size());
            } catch (Exception e) {
                log.error("Exception in task2", e);
            }
        };
        long initialDelayInSeconds = getInitialDelayInMillis(TrendBarPeriod.M1) / 1000;
        scheduledExecutorService.scheduleAtFixedRate(task1, initialDelayInSeconds + 80, 60, TimeUnit.SECONDS);
        scheduledExecutorService.scheduleAtFixedRate(task2, initialDelayInSeconds + 80, 60, TimeUnit.SECONDS);

        Thread.sleep(200_000 );
        var foundTrendBars = trendBarRepository.findAll();

        assertNotNull(foundTrendBars);
        assertFalse(foundTrendBars.isEmpty());

        log.info("foundTrendBars():");
        foundTrendBars.forEach(System.out::println);

        assertTrue(foundTrendBars.size() >= 4);
        log.info("foundTrendBars.size() = {}", foundTrendBars.size());
    }
}
