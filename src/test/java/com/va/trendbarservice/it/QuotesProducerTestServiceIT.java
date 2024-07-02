package com.va.trendbarservice.it;

import com.va.trendbarservice.model.Quote;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertFalse;

@Slf4j
public class QuotesProducerTestServiceIT {

    private QuotesProducerTestService underTest;

    @Mock
    private ConcurrentLinkedQueue<Quote> quotesQueue;

    @BeforeEach
    public void setUp() {
        quotesQueue = new ConcurrentLinkedQueue<>();
        underTest = new QuotesProducerTestService(quotesQueue);
        underTest.setINTERVAL(1000L);
    }

    @Test
    public void start() throws InterruptedException {
        underTest.init();
        underTest.start();

        Thread.sleep(2000);
        log.info("quotesQueue.size() = {}", quotesQueue.size());
        assertFalse(quotesQueue.isEmpty());
    }
}
