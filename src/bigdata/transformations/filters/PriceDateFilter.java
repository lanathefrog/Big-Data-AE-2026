package bigdata.transformations.filters;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;
import org.apache.spark.api.java.function.Function;

import java.time.Instant;

/**
 * This filter is used to filter prices that are outside of a specified date range.
 *
 * Only prices that are within the date range will be kept, and the rest will be filtered out.
 *
 */
public class PriceDateFilter implements Function<StockPrice, Boolean> {

    private final Instant minDate;
    private final Instant maxDate;

    public PriceDateFilter(Instant minDate, Instant maxDate) {
        this.minDate = minDate;
        this.maxDate = maxDate;
    }

    @Override
    public Boolean call(StockPrice price) {

        Instant priceDate = TimeUtil.fromDate(
                price.getYear(),
                price.getMonth(),
                price.getDay()
        );

        // Check if the price date is within the specified date range
        return !priceDate.isBefore(minDate) && !priceDate.isAfter(maxDate);
    }
}