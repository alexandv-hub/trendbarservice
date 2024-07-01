package com.va.trendbarservice.service;

import jakarta.annotation.PreDestroy;

public interface QuotesConsumerService {

    void start();

    @PreDestroy
    void shutdown();

}
