package com.va.trendbarservice.it.service;

import com.va.trendbarservice.model.Symbol;
import com.va.trendbarservice.model.TrendBarEntity;
import com.va.trendbarservice.model.TrendBarPeriod;
import com.va.trendbarservice.model.TrendBarStatus;
import com.va.trendbarservice.repository.TrendBarRepository;
import com.va.trendbarservice.service.impl.TrendBarHistoryServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Currency;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;


@Slf4j
@SpringBootTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@ActiveProfiles("test")
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class TrendBarHistoryServiceImplIT {

    @Autowired
    private TrendBarHistoryServiceImpl trendBarHistoryService;

    @Autowired
    private TrendBarRepository trendBarRepository;

    private Symbol symbolEURUSD;
    private TrendBarPeriod periodM1;
    private long timestampNow;
    private TrendBarEntity trendBarEntity1;
    private TrendBarEntity trendBarEntity2;

    @BeforeEach
    public void setUp() {
        symbolEURUSD = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        periodM1 = TrendBarPeriod.M1;
        timestampNow = Instant.now().toEpochMilli();

        trendBarEntity1 = TrendBarEntity.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .highPrice(BigDecimal.valueOf(1.1234).setScale(4, RoundingMode.HALF_UP))
                .lowPrice(BigDecimal.valueOf(1.1220).setScale(4, RoundingMode.HALF_UP))
                .openPrice(BigDecimal.valueOf(1.1225).setScale(4, RoundingMode.HALF_UP))
                .closePrice(BigDecimal.valueOf(1.1230).setScale(4, RoundingMode.HALF_UP))
                .timestamp(timestampNow - 60000) // 1 minute ago
                .period(periodM1)
                .status(TrendBarStatus.COMPLETED)
                .build();

        trendBarEntity2 = TrendBarEntity.builder()
                .id(null)
                .symbol(symbolEURUSD)
                .highPrice(BigDecimal.valueOf(1.1240).setScale(4, RoundingMode.HALF_UP))
                .lowPrice(BigDecimal.valueOf(1.1230).setScale(4, RoundingMode.HALF_UP))
                .openPrice(BigDecimal.valueOf(1.1235).setScale(4, RoundingMode.HALF_UP))
                .closePrice(BigDecimal.valueOf(1.1238).setScale(4, RoundingMode.HALF_UP))
                .timestamp(timestampNow) // now
                .period(periodM1)
                .status(TrendBarStatus.COMPLETED)
                .build();

        trendBarRepository.save(trendBarEntity1);
        trendBarRepository.save(trendBarEntity2);
    }

    @AfterEach
    public void tearDown() {
        trendBarRepository.deleteAll();
    }

    @Test
    public void givenValidSymbolAndPeriodAndTimestampRange_whenGetTrendBarsInRange_thenReturnTrendBars() {
        List<TrendBarEntity> trendBars = trendBarHistoryService.getTrendBarsBySymbolAndPeriodAndTimestampInRange(
                symbolEURUSD, periodM1, timestampNow - 120000, timestampNow);

        log.info("trendBarEntity1 = {}",trendBarEntity1);
        log.info("trendBarEntity2 = {}",trendBarEntity2);

        assertNotNull(trendBars);
        assertEquals(2, trendBars.size());
        for (TrendBarEntity trendBarEntity : trendBars) {
            System.out.println(trendBarEntity);
        }
        assertTrue(trendBars.contains(trendBarEntity1));
        assertTrue(trendBars.contains(trendBarEntity2));
    }

    @Test
    public void givenValidSymbolAndPeriodFrom_whenGetTrendBarsFrom_thenReturnTrendBars() {
        List<TrendBarEntity> trendBars = trendBarHistoryService.getTrendBarsBySymbolAndPeriodFrom(
                symbolEURUSD, periodM1, timestampNow - 120000);

        assertNotNull(trendBars);
        assertEquals(2, trendBars.size());
        assertTrue(trendBars.contains(trendBarEntity1));
        assertTrue(trendBars.contains(trendBarEntity2));
    }

    @Test
    public void givenZeroTimestampTo_whenGetTrendBarsInRange_thenReturnTrendBarsFrom() {
        List<TrendBarEntity> trendBars = trendBarHistoryService.getTrendBarsBySymbolAndPeriodAndTimestampInRange(
                symbolEURUSD, periodM1, timestampNow - 120000, 0);

        assertNotNull(trendBars);
        assertEquals(2, trendBars.size());
        assertTrue(trendBars.contains(trendBarEntity1));
        assertTrue(trendBars.contains(trendBarEntity2));
    }

    @Test
    public void givenNonExistingSymbol_whenGetTrendBarsInRange_thenReturnEmptyList() {
        Symbol nonExistingSymbol = Symbol.builder()
                .baseCurrency(Currency.getInstance("GBP"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        List<TrendBarEntity> trendBars = trendBarHistoryService.getTrendBarsBySymbolAndPeriodAndTimestampInRange(
                nonExistingSymbol, periodM1, timestampNow - 120000, timestampNow);

        assertNotNull(trendBars);
        assertTrue(trendBars.isEmpty());
    }
}
