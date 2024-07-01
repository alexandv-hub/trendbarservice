package com.va.trendbarservice.util;

import com.va.trendbarservice.model.TrendBarPeriod;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.time.temporal.ChronoUnit;


@Slf4j
public class TrendBarUtils {

    private TrendBarUtils() {

    }

    @Setter
    private static Instant fixedInstant;  // For testing

    public static long getInitialDelayInMillis(TrendBarPeriod period) {
        long now = getNow().toEpochMilli();
        long periodDurationInMillis = period.getDuration().toMillis();

        long startOfCurrentPeriod = (now / periodDurationInMillis) * periodDurationInMillis;
        long startOfNextPeriod = startOfCurrentPeriod + periodDurationInMillis;
        long initialDelayInMillis = startOfNextPeriod - now;

        log.info("Initial delay for period \"{}\" in millis = {}", period, initialDelayInMillis);
        return initialDelayInMillis;
    }

    // Method to get the instant, which can be fixed for testing
    private static Instant getNow() {
        return fixedInstant != null ? fixedInstant : Instant.now();
    }

    public static Instant getStartOfPeriod(Instant instant, TrendBarPeriod trendBarPeriod) {
        return switch (trendBarPeriod) {
            case M1 -> instant.truncatedTo(ChronoUnit.MINUTES);
            case H1 -> instant.truncatedTo(ChronoUnit.HOURS);
            case D1 -> instant.truncatedTo(ChronoUnit.DAYS);
        };
    }

    public static Instant getStartOfNextPeriod(Instant instant, TrendBarPeriod trendBarPeriod) {
        Instant startOfCurrentPeriod = getStartOfPeriod(instant, trendBarPeriod);
        return startOfCurrentPeriod.plus(trendBarPeriod.getDuration());
    }
}
