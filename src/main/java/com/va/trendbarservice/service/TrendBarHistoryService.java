package com.va.trendbarservice.service;

import com.va.trendbarservice.model.Symbol;
import com.va.trendbarservice.model.TrendBarEntity;
import com.va.trendbarservice.model.TrendBarPeriod;

import java.util.List;

public interface TrendBarHistoryService {

    List<TrendBarEntity> getTrendBarsBySymbolAndPeriodAndTimestampInRange(Symbol symbol, TrendBarPeriod period, long from, long to);

    List<TrendBarEntity> getTrendBarsBySymbolAndPeriodFrom(Symbol symbol, TrendBarPeriod period, long from);
}
