package bigdata.transformations.filters;

import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * After joining the features and metadata RDDs, this filter ensures that only
 * needed assets are passed for further proccessing.
 *
 * Conditions that are checked:
 * 1. The P/E ratio must be non-zero and below a specified threshold
 * 2. The asset volatility must be below a specified ceiling
 *
 * We also set the P/E ration into the AssetFeatures object, so that all the info
 * about the asset is stored in one single object.
 */

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
        // Remove assets with zero P/E ratio
        if (pe == 0.0) return false;


        // Store the P/E ratio in the features object
        features.setPeRatio(pe);


        // Apply the conditions for filtering
        return features.getAssetVolitility() < volatilityCeiling &&
                pe < peRatioThreshold;
    }
}