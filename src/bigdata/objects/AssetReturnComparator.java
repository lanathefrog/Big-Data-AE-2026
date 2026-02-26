package bigdata.objects;
import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;
import bigdata.objects.AssetFeatures;

public class AssetReturnComparator implements Comparator<Tuple2<String, AssetFeatures>>, Serializable {

    @Override
    public int compare(Tuple2<String, AssetFeatures> a,
                       Tuple2<String, AssetFeatures> b) {

        return Double.compare(
                b._2.getAssetReturn(),
                a._2.getAssetReturn()
        );
    }
}