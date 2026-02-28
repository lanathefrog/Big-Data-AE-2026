package bigdata.transformations.comparators;
import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;

/**
 * Comparator used to sort assets by return
 *
 * It orders assets in descending order of return, so that takeOrder(5, comparator)
 * will give the top 5 assets by return.
 *
 * It uses only AssetFeatures for the actual comparison, metadata is carried for the final output.
 */
public class AssetReturnComparatorWithMeta implements Comparator<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>>, Serializable {

    @Override
    public int compare(Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> a,
                       Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>> b) {

        return Double.compare(
                b._2._1.getAssetReturn(),
                a._2._1.getAssetReturn()
        );
    }
}