package com.va.trendbarservice.service.impl;

import com.va.trendbarservice.model.Symbol;
import com.va.trendbarservice.model.TrendBarEntity;
import com.va.trendbarservice.model.TrendBarPeriod;
import com.va.trendbarservice.repository.TrendBarRepository;
import com.va.trendbarservice.service.TrendBarHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;


@Slf4j
@Service
@RequiredArgsConstructor
public class TrendBarHistoryServiceImpl implements TrendBarHistoryService {

    private final TrendBarRepository trendBarRepository;

    @Override
    public List<TrendBarEntity> getTrendBarsBySymbolAndPeriodAndTimestampInRange(Symbol symbol, TrendBarPeriod trendBarPeriod, long from, long to) {
        if (to == 0) {
            return trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(symbol, trendBarPeriod, from)
                    .orElse(Collections.emptyList());
        }
        return trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(symbol, trendBarPeriod, from, to)
                .orElse(Collections.emptyList());
    }

    @Override
    public List<TrendBarEntity> getTrendBarsBySymbolAndPeriodFrom(Symbol symbol, TrendBarPeriod trendBarPeriod, long from) {
        return trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(symbol, trendBarPeriod, from)
                .orElse(Collections.emptyList());
    }
}
