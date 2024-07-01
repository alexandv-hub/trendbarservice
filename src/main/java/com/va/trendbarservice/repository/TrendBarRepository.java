package com.va.trendbarservice.repository;

import com.va.trendbarservice.model.Symbol;
import com.va.trendbarservice.model.TrendBarEntity;
import com.va.trendbarservice.model.TrendBarPeriod;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.Optional;

@Repository
public interface TrendBarRepository extends JpaRepository<TrendBarEntity, Long> {

    @Query("SELECT tb FROM TrendBarEntity tb WHERE tb.symbol = :symbol AND tb.period = :trendBarPeriod AND tb.timestamp BETWEEN :from AND :to")
    Optional<List<TrendBarEntity>> findTrendBarsBySymbolAndPeriodAndTimestampInRange(
            @Param("symbol") Symbol symbol,
            @Param("trendBarPeriod") TrendBarPeriod trendBarPeriod,
            @Param("from") long from,
            @Param("to") long to);

    @Query("SELECT tb FROM TrendBarEntity tb WHERE tb.symbol = :symbol AND tb.period = :trendBarPeriod AND tb.timestamp >= :from")
    Optional<List<TrendBarEntity>> findTrendBarsBySymbolAndPeriodFrom(
            @Param("symbol") Symbol symbol,
            @Param("trendBarPeriod") TrendBarPeriod trendBarPeriod,
            @Param("from") long from);
}
