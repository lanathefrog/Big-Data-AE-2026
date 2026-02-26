package bigdata.objects;
import org.apache.spark.api.java.function.Function;
import java.util.*;
import bigdata.util.TimeUtil;

public class SortAndExtractClosePrices implements Function<Iterable<StockPrice>, List<Double>> {

    @Override
    public List<Double> call(Iterable<StockPrice> prices) {

        List<StockPrice> priceList = new ArrayList<>();
        prices.forEach(priceList::add);

        priceList.sort(Comparator.comparing(
                p -> TimeUtil.fromDate(p.getYear(), p.getMonth(), p.getDay())
        ));

        List<Double> closePrices = new ArrayList<>();
        for (StockPrice p : priceList) {
            closePrices.add(p.getClosePrice());
        }

        return closePrices;
    }
}