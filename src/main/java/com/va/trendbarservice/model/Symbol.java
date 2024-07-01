package com.va.trendbarservice.model;

import lombok.*;
import java.util.Currency;

@Builder
public record Symbol(Currency baseCurrency, Currency quoteCurrency) {

    @Override
    public String toString() {
        return baseCurrency.toString() + quoteCurrency;
    }
}
