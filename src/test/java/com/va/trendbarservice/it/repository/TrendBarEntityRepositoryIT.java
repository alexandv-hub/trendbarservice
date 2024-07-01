package com.va.trendbarservice.it.repository;

import com.va.trendbarservice.model.*;
import com.va.trendbarservice.repository.TrendBarRepository;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.BigDecimalComparator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Currency;
import java.util.Objects;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.*;


@Slf4j
@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
public class TrendBarEntityRepositoryIT {

    @Autowired
    private TrendBarRepository trendBarRepository;

    private static final Symbol SYMBOL_EURUSD = Symbol.builder()
            .baseCurrency(Currency.getInstance("EUR"))
            .quoteCurrency(Currency.getInstance("USD"))
            .build();

    private static TrendBarEntity getTrendBar() {
        return TrendBarEntity.builder()
                .id(null)
                .symbol(SYMBOL_EURUSD)
                .highPrice(BigDecimal.valueOf(100.00))
                .lowPrice(BigDecimal.valueOf(20.15))
                .closePrice(BigDecimal.valueOf(50.50))
                .openPrice(BigDecimal.valueOf(10.99))
                .period(TrendBarPeriod.M1)
                .timestamp(System.currentTimeMillis())
                .status(TrendBarStatus.COMPLETED)
                .build();
    }

    @AfterEach
    public void tearDown() {
        trendBarRepository.deleteAll();
    }


    @Test
    public void givenTrendBar_whenSaved_thenCanBeFoundById() {
        var testTrendBar = getTrendBar();

        var savedTrendBar = trendBarRepository.save(testTrendBar);
        var foundTrendBar = trendBarRepository.findById(savedTrendBar.getId()).orElse(null);

        assertNotNull(foundTrendBar);
        assertEquals(savedTrendBar.getId(), Objects.requireNonNull(foundTrendBar).getId());
    }

    @Test
    public void givenTrendBar_whenSaved_thenCanBeFoundByIdAndAllFieldsCorrect() {
        var testTrendBar = getTrendBar();

        var savedTrendBar = trendBarRepository.save(testTrendBar);
        var foundTrendBar = trendBarRepository.findById(savedTrendBar.getId()).orElse(null);

        assertNotNull(foundTrendBar);
        assertThat(foundTrendBar)
                .usingComparatorForType(BigDecimalComparator.BIG_DECIMAL_COMPARATOR, BigDecimal.class)
                .usingRecursiveComparison()
                .isEqualTo(savedTrendBar);
    }


    @Test
    public void given2TrendBarsInRange_whenFindTrendBarsBySymbolAndPeriodAndTimestampInRange_thenReturns2TrendBars() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-16 00:00:01").getTime());

        var trendBar2 = getTrendBar();
        trendBar2.setTimestamp(Timestamp.valueOf("2024-06-16 01:00:00").getTime());

        trendBarRepository.save(trendBar1);
        trendBarRepository.save(trendBar2);

        long from = Timestamp.valueOf("2024-06-16 00:00:00").getTime();
        long to = Timestamp.valueOf("2024-06-16 02:00:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from,
                to
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(2, foundTrendBars.size());
    }

    @Test
    public void given2TrendBarsInAnd2TrendBarsOutOfRange_whenFindTrendBarsBySymbolAndPeriodAndTimestampInRange_thenReturns2TrendBars() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-15 00:00:00").getTime());

        var trendBar2 = getTrendBar();
        trendBar2.setTimestamp(Timestamp.valueOf("2024-06-16 00:00:01").getTime());

        var trendBar3 = getTrendBar();
        trendBar3.setTimestamp(Timestamp.valueOf("2024-06-16 00:01:00").getTime());

        var trendBar4 = getTrendBar();
        trendBar4.setTimestamp(Timestamp.valueOf("2024-06-17 00:00:00").getTime());

        trendBarRepository.save(trendBar1);
        trendBarRepository.save(trendBar2);
        trendBarRepository.save(trendBar3);
        trendBarRepository.save(trendBar4);

        long from = Timestamp.valueOf("2024-06-16 00:00:00").getTime();
        long to = Timestamp.valueOf("2024-06-16 00:02:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from,
                to
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(2, foundTrendBars.size());
    }

    @Test
    public void given2TrendBarsOutOfRange_whenFindTrendBarsBySymbolAndPeriodAndTimestampInRange_thenReturnsEmptyList() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-15 00:00:01").getTime());

        var trendBar2 = getTrendBar();
        trendBar2.setTimestamp(Timestamp.valueOf("2024-06-16 00:03:00").getTime());

        trendBarRepository.save(trendBar1);
        trendBarRepository.save(trendBar2);

        long from = Timestamp.valueOf("2024-06-12 00:00:00").getTime();
        long to = Timestamp.valueOf("2024-06-13 00:00:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from,
                to
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(0, foundTrendBars.size());
    }

    @Test
    public void givenTrendBarWithZeroTime_whenFindTrendBarsBySymbolAndPeriodAndTimestampInRange_thenReturnsTrendBar() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-16 00:00:00").getTime());

        trendBarRepository.save(trendBar1);

        long from = Timestamp.valueOf("2024-06-16 00:00:00").getTime();
        long to = Timestamp.valueOf("2024-06-16 00:01:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodAndTimestampInRange(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from,
                to
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(1, foundTrendBars.size());
    }


    @Test
    public void given2TrendBarsInRange_whenFindTrendBarsBySymbolAndPeriodFrom_thenReturns2TrendBars() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-16 00:00:01").getTime());

        var trendBar2 = getTrendBar();
        trendBar2.setTimestamp(Timestamp.valueOf("2024-06-16 01:00:00").getTime());

        trendBarRepository.save(trendBar1);
        trendBarRepository.save(trendBar2);

        long from = Timestamp.valueOf("2024-06-16 00:00:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(2, foundTrendBars.size());
    }

    @Test
    public void given2TrendBarsInAnd2TrendBarsOutOfRange_whenFindTrendBarsBySymbolAndPeriodFrom_thenReturns2TrendBars() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-15 00:00:00").getTime());

        var trendBar2 = getTrendBar();
        trendBar2.setTimestamp(Timestamp.valueOf("2024-06-15 00:00:01").getTime());

        var trendBar3 = getTrendBar();
        trendBar3.setTimestamp(Timestamp.valueOf("2024-06-16 00:00:01").getTime());

        var trendBar4 = getTrendBar();
        trendBar4.setTimestamp(Timestamp.valueOf("2024-06-16 00:00:02").getTime());

        trendBarRepository.save(trendBar1);
        trendBarRepository.save(trendBar2);
        trendBarRepository.save(trendBar3);
        trendBarRepository.save(trendBar4);

        long from = Timestamp.valueOf("2024-06-16 00:00:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(2, foundTrendBars.size());
    }

    @Test
    public void given2TrendBarsOutOfRange_whenFindTrendBarsBySymbolAndPeriodFrom_thenReturnsEmptyList() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-14 00:00:00").getTime());

        var trendBar2 = getTrendBar();
        trendBar2.setTimestamp(Timestamp.valueOf("2024-06-14 00:00:01").getTime());

        trendBarRepository.save(trendBar1);
        trendBarRepository.save(trendBar2);

        long from = Timestamp.valueOf("2024-06-15 00:00:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(0, foundTrendBars.size());
    }

    @Test
    public void givenTrendBarWithZeroTime_whenFindTrendBarsBySymbolAndPeriodFrom_thenReturnsTrendBar() {
        var trendBar1 = getTrendBar();
        trendBar1.setTimestamp(Timestamp.valueOf("2024-06-16 00:00:00").getTime());

        trendBarRepository.save(trendBar1);

        long from = Timestamp.valueOf("2024-06-16 00:00:00").getTime();

        var foundTrendBars = trendBarRepository.findTrendBarsBySymbolAndPeriodFrom(
                trendBar1.getSymbol(),
                trendBar1.getPeriod(),
                from
        ).orElse(null);

        assertNotNull(foundTrendBars);
        assertEquals(1, foundTrendBars.size());
    }
}
