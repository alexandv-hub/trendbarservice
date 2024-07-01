package com.va.trendbarservice.util;

import com.va.trendbarservice.model.Symbol;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.Currency;

import static com.va.trendbarservice.messages.ExceptionMessages.INVALID_DATA_FOR_SYMBOL;

@Converter(autoApply = true)
public class SymbolConverter implements AttributeConverter<Symbol, String> {

    @Override
    public String convertToDatabaseColumn(Symbol symbol) {
        if (symbol == null) {
            return null;
        }
        return symbol.toString();
    }

    @Override
    public Symbol convertToEntityAttribute(String dbData) {
        if (dbData == null || dbData.length() != 6) {
            throw new IllegalArgumentException(INVALID_DATA_FOR_SYMBOL + dbData);
        }
        String baseCurrencyCode = dbData.substring(0, 3);
        String quoteCurrencyCode = dbData.substring(3, 6);
        return new Symbol(Currency.getInstance(baseCurrencyCode), Currency.getInstance(quoteCurrencyCode));
    }
}