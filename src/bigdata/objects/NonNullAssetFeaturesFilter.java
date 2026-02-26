package bigdata.objects;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class NonNullAssetFeaturesFilter implements Function<Tuple2<String, AssetFeatures>, Boolean> {

    @Override
    public Boolean call(Tuple2<String, AssetFeatures> x) {
        return x._2 != null;
    }
}