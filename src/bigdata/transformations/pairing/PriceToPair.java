package bigdata.transformations.pairing;
import bigdata.objects.StockPrice;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class PriceToPair implements PairFunction<StockPrice, String, StockPrice> {

    @Override
    public Tuple2<String, StockPrice> call(StockPrice price) {
        return new Tuple2<>(price.getStockTicker(), price);
    }
}