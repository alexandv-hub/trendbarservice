package com.va.trendbarservice.service;

import com.va.trendbarservice.model.TrendBar;
import jakarta.annotation.PostConstruct;
import org.springframework.transaction.annotation.Transactional;


public interface TrendBarBuilderService {

    @PostConstruct
    void initMaps();

    @Transactional
    void buildTrendBar(TrendBar trendBar);

}
