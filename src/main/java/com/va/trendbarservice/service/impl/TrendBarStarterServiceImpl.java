package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.service.*;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Slf4j
@Service
@RequiredArgsConstructor
public class TrendBarStarterServiceImpl implements TrendBarStarterService {

    private final QuotesConsumerService quotesConsumerService;
    private final TrendBarSchedulerService schedulerService;
    private final TrendBarBuilderService trendBarBuilderService;

    @PostConstruct
    public void init() {
        trendBarBuilderService.initMaps();
    }

    @PreDestroy
    public void shutdown() {
        schedulerService.shutDown();
    }

    @Override
    @Transactional
    public void start() {
        log.info("Starting TrendBarBuilderServiceImpl.start()...");
        schedulerService.startAllTrendBarBuildersWithInitialDelays();
        schedulerService.startQuotesConsumer();
    }

    @Override
    @PreDestroy
    public void shutDown() {
        schedulerService.shutDown();
        quotesConsumerService.shutdown();
        log.info("TrendBarService stopped successfully");
    }

}
