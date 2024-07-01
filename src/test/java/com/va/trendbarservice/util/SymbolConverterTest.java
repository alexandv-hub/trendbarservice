package com.va.trendbarservice.util;

import com.va.trendbarservice.model.Symbol;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import java.util.Currency;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
class SymbolConverterTest {

    private final SymbolConverter converter = new SymbolConverter();

    @Test
    public void givenSymbol_whenConvertToDatabaseColumn_thenReturnsCorrectStringEURUSD() {
        Symbol symbol = Symbol.builder()
                .baseCurrency(Currency.getInstance("EUR"))
                .quoteCurrency(Currency.getInstance("USD"))
                .build();

        String result = converter.convertToDatabaseColumn(symbol);
        log.info("testConvertToDatabaseColumn: result = {}", result);

        assertEquals("EURUSD", result);
    }

    @Test
    public void givenNull_whenConvertToDatabaseColumn_thenReturnsNull() {
        String result = converter.convertToDatabaseColumn(null);
        log.info("testConvertToDatabaseColumn_Null: result = {}", result);

        assertNull(result);
    }

    @Test
    public void givenString_whenTestConvertToEntityAttribute_thenReturnsCorrectStringEURUSD() {
        String dbData = "EURUSD";
        Symbol result = converter.convertToEntityAttribute(dbData);
        log.info("testConvertToEntityAttribute: result = {}", result);

        assertEquals("EURUSD", result.toString());
    }

    @Test
    public void givenNull_whenConvertToEntityAttribute_thenReturnsThrowsException() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute(null));
        log.info("testConvertToEntityAttribute_null: exception = {}", exception.getMessage());
        assertEquals("Invalid data for Symbol: null", exception.getMessage());
    }

    @Test
    public void givenInvalidLengthSymbol_whenConvertToEntityAttribute_thenThrowsException() {
        String dbData = "EURUS";
        Exception exception = assertThrows(IllegalArgumentException.class, () -> converter.convertToEntityAttribute(dbData));

        log.info("testConvertToEntityAttribute_InvalidLength: exception = {}", exception.getMessage());
        assertEquals("Invalid data for Symbol: EURUS", exception.getMessage());
    }

    @Test
    public void givenValidSymbol_whenConvertToEntityAttribute_thenReturnsSymbol() {
        String dbData = "EURUSD";
        Symbol result = converter.convertToEntityAttribute(dbData);
        log.info("testConvertToEntityAttribute_ValidSymbol: result = {}", result);

        assertNotNull(result);
        assertEquals("EUR", result.baseCurrency().getCurrencyCode());
        assertEquals("USD", result.quoteCurrency().getCurrencyCode());
    }

    @Test
    public void givenSymbol_testConvertToEntityAttribute_returnsCorrectEURJPY() {
        String dbData = "EURJPY";
        Symbol result = converter.convertToEntityAttribute(dbData);
        log.info("testConvertToEntityAttribute_EURJPY: result = {}", result);

        assertEquals("EURJPY", result.toString());
    }
}
