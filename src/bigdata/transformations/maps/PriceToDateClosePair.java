package bigdata.transformations.maps;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.time.Instant;

/**
 * Converts a StockPrice object into a pair of (stock ticker, (date, close price)).
 */

public class PriceToDateClosePair implements
        PairFunction<StockPrice, String, Tuple2<Instant, Double>> {

    @Override
    public Tuple2<String, Tuple2<Instant, Double>> call(StockPrice p) {

        // Convert the year, month, and day fields of the StockPrice object into a single Instant representing the date
        Instant date = TimeUtil.fromDate(
                p.getYear(),
                p.getMonth(),
                p.getDay()
        );

        // Create a pair of (stock ticker, (date, close price))
        return new Tuple2<>(p.getStockTicker(), new Tuple2<>(date, p.getClosePrice()));
    }
}