package com.va.trendbarservice.service;

import jakarta.annotation.PreDestroy;
import org.springframework.transaction.annotation.Transactional;

public interface TrendBarStarterService {

    @Transactional
    void start();

    @PreDestroy
    void shutDown();
}
