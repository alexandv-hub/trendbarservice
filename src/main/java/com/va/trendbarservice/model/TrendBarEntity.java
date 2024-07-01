package com.va.trendbarservice.model;

import com.va.trendbarservice.util.SymbolConverter;
import jakarta.persistence.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Entity
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class TrendBarEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Convert(converter = SymbolConverter.class)
    @Column(nullable = false)
    private Symbol symbol;

    @Column(nullable = false, precision = 10, scale = 4)
    private BigDecimal openPrice;

    @Column(nullable = false, precision = 10, scale = 4)
    private BigDecimal closePrice;

    @Column(nullable = false, precision = 10, scale = 4)
    private BigDecimal highPrice;

    @Column(nullable = false, precision = 10, scale = 4)
    private BigDecimal lowPrice;

    @Column(nullable = false)
    private TrendBarPeriod period;

    @Column(nullable = false)
    private long timestamp;

    @Column(nullable = false)
    private TrendBarStatus status;

    @ToString.Include(name = "timestamp")
    public String getFormattedTimestamp() {
        return Instant.ofEpochMilli(timestamp)
                .atZone(ZoneId.systemDefault())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
}
