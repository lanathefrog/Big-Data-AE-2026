package bigdata.transformations.filters;
import bigdata.objects.AssetFeatures;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * This filter is used to remove assets that do not have any features calculated (
 *
 *
 */
public class NonNullAssetFeaturesFilter implements Function<Tuple2<String, AssetFeatures>, Boolean> {

    @Override
    public Boolean call(Tuple2<String, AssetFeatures> x) {
        return x._2 != null;
    }
}