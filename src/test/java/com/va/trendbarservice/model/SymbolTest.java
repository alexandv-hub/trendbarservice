package com.va.trendbarservice.model;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import java.util.Currency;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class SymbolTest {

    @Test
    public void givenSymbol_whenToString_returnsCorrectEURUSD() {
        var symbol = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        String result = symbol.toString();
        log.info("testToString_EURUSD: result = {}", result);

        assertEquals(result,"EURUSD");
    }

    @Test
    public void givenSymbol_whenToString_returnsCorrectEURJPY() {
        var symbol = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("JPY"))
                .build();

        String result = symbol.toString();
        log.info("testToString_EURJPY: result = {}", result);

        assertEquals(result,"EURJPY");
    }

    @Test
    public void givenSymbol_whenToString_returnsCorrectUSDEUR() {
        var symbol = Symbol.builder()
                .baseCurrency(Currency.getInstance("USD"))
                .quoteCurrency(Currency.getInstance("EUR"))
                .build();

        String result = symbol.toString();
        log.info("testToString_USDEUR: result = {}", result);

        assertEquals(result, "USDEUR");
    }
}
