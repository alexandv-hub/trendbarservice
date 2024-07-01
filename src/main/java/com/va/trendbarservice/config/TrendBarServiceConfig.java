package com.va.trendbarservice.config;

import com.va.trendbarservice.model.Quote;
import com.va.trendbarservice.model.TrendBar;
import com.va.trendbarservice.model.TrendBarEntity;
import com.va.trendbarservice.model.TrendBarKey;
import com.va.trendbarservice.util.MicroBatcher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Optional;
import java.util.concurrent.*;


@Slf4j
@Configuration
public class TrendBarServiceConfig {

    @Bean
    public ConcurrentLinkedQueue<Quote> quotesQueue() {
        return new ConcurrentLinkedQueue<>();
    }

    @Bean
    public ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<TrendBar, Optional<ScheduledFuture<?>>> currBuildersMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<TrendBar, MicroBatcher> currMicroBatchersMap() {
        return new ConcurrentHashMap<>();
    }


    @Bean
    public ConcurrentMap<TrendBar, TrendBarEntity> currTrendBarEntitiesMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ExecutorService consumerExecutorService() {
        return Executors.newSingleThreadScheduledExecutor();
    }

    @Bean
    public ScheduledExecutorService scheduler(ConcurrentMap<TrendBarKey, LinkedBlockingQueue<Quote>> keyToQuotesQueueMap) {
        int keyCount = keyToQuotesQueueMap.size();
        int corePoolSize = Math.min(1 + keyCount, Runtime.getRuntime().availableProcessors());
        log.info("Initializing scheduler with core pool size: {}", corePoolSize);
        return Executors.newScheduledThreadPool(corePoolSize);
    }
}
