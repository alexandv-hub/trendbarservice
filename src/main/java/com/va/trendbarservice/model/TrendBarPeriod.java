package com.va.trendbarservice.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.time.Duration;

@Getter
@AllArgsConstructor
public enum TrendBarPeriod {
    M1(Duration.ofMinutes(1)),
    H1(Duration.ofHours(1)),
    D1(Duration.ofDays(1));

    private final Duration duration;
}

