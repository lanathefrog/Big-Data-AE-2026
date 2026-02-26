package bigdata.objects;
import java.io.Serializable;
import java.util.Comparator;
import scala.Tuple2;
import bigdata.objects.AssetFeatures;
import bigdata.objects.AssetMetadata;

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