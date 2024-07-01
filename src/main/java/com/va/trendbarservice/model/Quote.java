package com.va.trendbarservice.model;

import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Quote {
    private Long id;

    private Symbol symbol;
    private BigDecimal newPrice;
    private long unixTimeStamp;

    @ToString.Include(name = "unixTimeStamp")
    public String getFormattedTimestamp() {
        return Instant.ofEpochMilli(unixTimeStamp)
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
