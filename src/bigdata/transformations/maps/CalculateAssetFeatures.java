package bigdata.transformations.maps;

import bigdata.objects.AssetFeatures;
import org.apache.spark.api.java.function.Function;
import java.util.List;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;

/**
 * Calculates indicators required for the ranking an asset.
 *
 * Input: List of close prices for an asset
 * Output: AssetFeatures object containing the return and volatility of the asset
 *
 * Requires at least 251 trading days, otherwise return null (remove later in the pipeline)
 * Full history is used to calculate the return
 * Violatility is calculated using the last 251 trading days
 */
public class CalculateAssetFeatures implements Function<List<Double>, AssetFeatures> {

    @Override
    public AssetFeatures call(List<Double> closePrices) {

        if (closePrices.size() < 251) return null;

        Returns returnsCalc = new Returns();
        double assetReturn = returnsCalc.calculate(5, closePrices);

        List<Double> last251 = closePrices.subList(closePrices.size() - 251, closePrices.size());

        Volitility volCalc = new Volitility();
        double assetVol = volCalc.calculate(last251);

        AssetFeatures features = new AssetFeatures();
        features.setAssetReturn(assetReturn);
        features.setAssetVolitility(assetVol);

        return features;
    }
}