package com.va.trendbarservice.util;

import com.va.trendbarservice.model.TrendBarPeriod;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static com.va.trendbarservice.util.TrendBarUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class TrendBarEntityUtilsTest {

    @Test
    public void givenPeriodOfOneMinute_whenGetInitialDelayInMillis_thenCorrectInitialDelay() {
        var trendBarPeriod = TrendBarPeriod.M1;
        Instant fixedNow = Instant.parse("2024-06-20T00:00:00Z");
        TrendBarUtils.setFixedInstant(fixedNow);
        long expectedDelay = Duration.ofMinutes(1).toMillis();

        long actualDelay = getInitialDelayInMillis(trendBarPeriod);

        assertEquals(expectedDelay, actualDelay);
    }

    @Test
    public void givenPeriodOfOneHour_whenGetInitialDelayInMillis_thenCorrectInitialDelay() {
        var trendBarPeriod = TrendBarPeriod.H1;
        Instant fixedNow = Instant.parse("2024-06-20T00:00:00Z");
        TrendBarUtils.setFixedInstant(fixedNow);
        long expectedDelay = Duration.ofHours(1).toMillis();

        long actualDelay = getInitialDelayInMillis(trendBarPeriod);

        assertEquals(expectedDelay, actualDelay);
    }


    @Test
    public void givenPeriodOfOneDay_whenGetInitialDelayInMillis_thenCorrectInitialDelay() {
        var trendBarPeriod = TrendBarPeriod.D1;
        Instant fixedNow = Instant.parse("2024-06-20T00:00:00Z");
        TrendBarUtils.setFixedInstant(fixedNow);
        long expectedDelay = Duration.ofDays(1).toMillis();

        long actualDelay = getInitialDelayInMillis(trendBarPeriod);

        assertEquals(expectedDelay, actualDelay);
    }


    @Test
    public void givenPeriodOfOneMinuteNearEndOfMinute_whenGetInitialDelayInMillis_thenCorrectInitialDelay() {
        var trendBarPeriod = TrendBarPeriod.M1;
        Instant fixedNow = Instant.parse("2024-06-20T00:00:59Z");
        TrendBarUtils.setFixedInstant(fixedNow);
        long expectedDelay = Duration.ofSeconds(1).toMillis();

        long actualDelay = getInitialDelayInMillis(trendBarPeriod);

        assertEquals(expectedDelay, actualDelay);
    }



    @Test
    public void givenPeriodM1_whenGetStartOfPeriod_thenReturnsCorrectStartOfPeriod() {
        Instant now = Instant.parse("2024-06-16T07:30:45.123Z");
        Instant expected = Instant.parse("2024-06-16T07:30:00.000Z");

        Instant result = getStartOfPeriod(now, TrendBarPeriod.M1);

        assertEquals(expected, result);
    }

    @Test
    public void givenPeriodH1_whenGetStartOfPeriod_thenReturnsCorrectStartOfPeriod() {
        Instant now = Instant.parse("2024-06-16T07:30:45.123Z");
        Instant expected = Instant.parse("2024-06-16T07:00:00.000Z");

        Instant result = getStartOfPeriod(now, TrendBarPeriod.H1);

        assertEquals(expected, result);
    }

    @Test
    public void givenPeriodD1_whenGetStartOfPeriod_thenReturnsCorrectStartOfPeriod() {
        Instant now = Instant.parse("2024-06-16T07:30:45.123Z");
        Instant expected = Instant.parse("2024-06-16T00:00:00.000Z");

        Instant result = getStartOfPeriod(now, TrendBarPeriod.D1);

        assertEquals(expected, result);
    }

    @Test
    public void givenPeriodNull_whenGetStartOfPeriod_thenThrowsUnsupported() {
        Instant now = Instant.now();
        assertThrows(NullPointerException.class, () -> getStartOfPeriod(now, null));
    }


    @Test
    public void givenPeriodM1_whenGetStartOfNextPeriod_thenReturnsCorrectStartOfNextPeriod() {
        Instant now = Instant.parse("2024-06-21T10:30:45.123Z");
        Instant expected = Instant.parse("2024-06-21T10:31:00.000Z");

        Instant result = getStartOfNextPeriod(now, TrendBarPeriod.M1);

        assertEquals(expected, result);
    }

    @Test
    public void givenPeriodH1_whenGetStartOfNextPeriod_thenReturnsCorrectStartOfNextPeriod() {
        Instant now = Instant.parse("2024-06-21T10:30:45.123Z");
        Instant expected = Instant.parse("2024-06-21T11:00:00.000Z");

        Instant result = getStartOfNextPeriod(now, TrendBarPeriod.H1);

        assertEquals(expected, result);
    }

    @Test
    public void givenPeriodD1_whenGetStartOfNextPeriod_thenReturnsCorrectStartOfNextPeriod() {
        Instant now = Instant.parse("2024-06-21T10:30:45.123Z");
        Instant expected = Instant.parse("2024-06-22T00:00:00.000Z");

        Instant result = getStartOfNextPeriod(now, TrendBarPeriod.D1);

        assertEquals(expected, result);
    }

    @Test
    public void givenPeriodNull_whenGetStartOfNextPeriod_thenThrowsUnsupported() {
        Instant now = Instant.now();
        assertThrows(NullPointerException.class, () -> getStartOfNextPeriod(now, null));
    }
}
