package bigdata.transformations.maps;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Sorts all the (date, close price) pairs by date and then extracts the close prices into a list.
 */
public class SortAndExtractClosePricesFromTuple
        implements Function<ArrayList<Tuple2<Instant, Double>>, List<Double>> {

    @Override
    public List<Double> call(ArrayList<Tuple2<Instant, Double>> prices) {

        // Sort the list of (date, close price) pairs by date
        prices.sort(Comparator.comparing(Tuple2::_1));

        List<Double> closePrices = new ArrayList<>(prices.size());

        // Extract the close prices from the sorted list of (date, close price) pairs
        for (Tuple2<Instant, Double> p : prices) {
            closePrices.add(p._2);
        }

        return closePrices;
    }
}