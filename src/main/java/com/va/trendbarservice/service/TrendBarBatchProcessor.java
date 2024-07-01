package com.va.trendbarservice.service;

import com.va.trendbarservice.model.Quote;
import com.va.trendbarservice.model.TrendBar;

import java.util.List;

public interface TrendBarBatchProcessor {

    void processMicroBatch(List<Quote> quotesBatch, TrendBar trendBar, boolean isBatchFinal);

    void shutdownAllMicroBatchers();

}
