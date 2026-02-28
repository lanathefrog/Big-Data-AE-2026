package bigdata.transformations.filters;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;
import org.apache.spark.api.java.function.Function;

import java.time.Instant;

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

        return !priceDate.isBefore(minDate) && !priceDate.isAfter(maxDate);
    }
}