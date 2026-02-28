package bigdata.transformations.maps;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SortAndExtractClosePrices
        implements Function<ArrayList<StockPrice>, List<Double>> {

    @Override
    public List<Double> call(ArrayList<StockPrice> priceList) {

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