package bigdata.objects;

import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class AssetFilterWithMeta implements Function<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>, Boolean> {

    private final double volatilityCeiling;
    private final double peRatioThreshold;

    public AssetFilterWithMeta(double volatilityCeiling, double peRatioThreshold) {
        this.volatilityCeiling = volatilityCeiling;
        this.peRatioThreshold = peRatioThreshold;
    }

    @Override
    public Boolean call(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> x) {

        AssetFeatures features = x._2._1;
        AssetMetadata metadata = x._2._2;

        double pe = metadata.getPriceEarningRatio();
        if (pe == 0.0) return false;

        features.setPeRatio(pe);

        return features.getAssetVolitility() < volatilityCeiling &&
                pe < peRatioThreshold;
    }
}