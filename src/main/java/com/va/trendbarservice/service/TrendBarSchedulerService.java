package com.va.trendbarservice.service;

import com.va.trendbarservice.model.TrendBar;
import jakarta.annotation.PreDestroy;

public interface TrendBarSchedulerService {

    void startQuotesConsumer();

    void startAllTrendBarBuildersWithInitialDelays();

    void startScheduledTrendBarBuild(TrendBar trendBar, long initialDelayInMillis);

    @PreDestroy
    void shutDown();

}
