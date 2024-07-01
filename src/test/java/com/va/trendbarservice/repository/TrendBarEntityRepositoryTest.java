package com.va.trendbarservice.repository;

import com.va.trendbarservice.model.Symbol;
import com.va.trendbarservice.model.TrendBarEntity;
import com.va.trendbarservice.model.TrendBarPeriod;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Timestamp;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TrendBarEntityRepositoryTest {

    @Mock
    private TrendBarRepository trendBarRepository;

    private Symbol testSymbol;
    private TrendBarPeriod testPeriod;

    @BeforeEach
    public void setUp() {
        testSymbol = new Symbol(Currency.getInstance("EUR"), Currency.getInstance("USD"));
        testPeriod = TrendBarPeriod.M1;
    }


    @Test
    public void givenValidInputs_whenFindTrendBarsBySymbolAndPeriodAndTimestampInRange_thenReturnsTrendBars() {
        long fromTimestamp = Timestamp.valueOf("2024-06-16 00:00:00").getTime();
        long toTimestamp = Timestamp.valueOf("2024-06-16 00:03:00").getTime();

        var trendBar1 = TrendBarEntity.builder()
                .symbol(testSymbol)
                .period(testPeriod)
                .timestamp(fromTimestamp + 1000)
                .build();
        var trendBar2 = TrendBarEntity.builder()
                .symbol(testSymbol)
                .period(testPeriod)
                .timestamp(toTimestamp - 1000)
                .build();
        List<TrendBarEntity> expectedTrendBarEntities = List.of(trendBar1, trendBar2);

        when(trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp))
                .thenReturn(Optional.of(expectedTrendBarEntities));

        Optional<List<TrendBarEntity>> result = trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp);

        assertTrue(result.isPresent());
        assertEquals(2, result.get().size());
        verify(trendBarRepository, times(1)).findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp);
    }

    @Test
    public void givenNoTrendBars_whenFindTrendBarsBySymbolAndPeriodAndTimestampInRange_thenReturnsEmpty() {
        long fromTimestamp = Timestamp.valueOf("2024-06-16 00:00:00").getTime();
        long toTimestamp = Timestamp.valueOf("2024-06-16 00:01:00").getTime();

        when(trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp))
                .thenReturn(Optional.empty());

        Optional<List<TrendBarEntity>> result = trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp);

        assertFalse(result.isPresent());
        verify(trendBarRepository, times(1)).findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp);
    }

    @Test
    public void givenBoundaryTimestamps_whenFindTrendBarsBySymbolAndPeriodAndTimestampInRange_thenReturnsTrendBars() {
        long fromTimestamp = Long.MIN_VALUE;
        long toTimestamp = Long.MAX_VALUE;

        var trendBar = TrendBarEntity.builder()
                .symbol(testSymbol)
                .period(testPeriod)
                .timestamp(fromTimestamp + 1)
                .build();
        List<TrendBarEntity> expectedTrendBarEntities = List.of(trendBar);

        when(trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp))
                .thenReturn(Optional.of(expectedTrendBarEntities));

        Optional<List<TrendBarEntity>> result = trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        verify(trendBarRepository, times(1)).findTrendBarsBySymbolAndPeriodAndTimestampInRange(testSymbol, testPeriod, fromTimestamp, toTimestamp);
    }


    @Test
    public void givenValidInputs_whenFindTrendBarsBySymbolAndPeriodFrom_thenReturnsTrendBars() {
        long fromTimestamp = Timestamp.valueOf("2024-06-16 00:00:00").getTime();

        var trendBar1 = TrendBarEntity.builder()
                .symbol(testSymbol)
                .period(testPeriod)
                .timestamp(fromTimestamp + 1000)
                .build();
        var trendBar2 = TrendBarEntity.builder()
                .symbol(testSymbol)
                .period(testPeriod)
                .timestamp(fromTimestamp + 2000)
                .build();
        List<TrendBarEntity> expectedTrendBarEntities = Arrays.asList(trendBar1, trendBar2);

        when(trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp))
                .thenReturn(Optional.of(expectedTrendBarEntities));

        Optional<List<TrendBarEntity>> result = trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp);

        assertTrue(result.isPresent());
        assertEquals(2, result.get().size());
        verify(trendBarRepository, times(1)).findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp);
    }

    @Test
    public void givenNoTrendBars_whenFindTrendBarsBySymbolAndPeriodFrom_thenReturnsEmpty() {
        long fromTimestamp = Timestamp.valueOf("2024-06-16 00:00:00").getTime();

        when(trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp))
                .thenReturn(Optional.empty());

        Optional<List<TrendBarEntity>> result = trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp);

        assertFalse(result.isPresent());
        verify(trendBarRepository, times(1)).findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp);
    }

    @Test
    public void givenBoundaryTimestamps_whenFindTrendBarsBySymbolAndPeriodFrom_thenReturnsTrendBars() {
        long fromTimestamp = Long.MIN_VALUE;

        var trendBar = TrendBarEntity.builder()
                .symbol(testSymbol)
                .period(testPeriod)
                .timestamp(fromTimestamp + 1)
                .build();
        List<TrendBarEntity> expectedTrendBarEntities = Collections.singletonList(trendBar);

        when(trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp))
                .thenReturn(Optional.of(expectedTrendBarEntities));

        Optional<List<TrendBarEntity>> result = trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp);

        assertTrue(result.isPresent());
        assertEquals(1, result.get().size());
        verify(trendBarRepository, times(1)).findTrendBarsBySymbolAndPeriodFrom(testSymbol, testPeriod, fromTimestamp);
    }
}
