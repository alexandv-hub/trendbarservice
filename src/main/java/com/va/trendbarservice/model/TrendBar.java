package com.va.trendbarservice.model;

import lombok.Builder;

import java.time.Instant;

@Builder(toBuilder = true)
public record TrendBar(TrendBarKey trendBarKey, Instant startOfPeriod) {}
