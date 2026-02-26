package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.Date;

import bigdata.objects.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.pairing.AssetMetadataPairing;
import scala.Tuple2;
import bigdata.util.TimeUtil;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import bigdata.technicalindicators.Volitility;
import bigdata.technicalindicators.Returns;
public class AssessedExercise {

public static void main(String[] args) throws InterruptedException {

		
		//--------------------------------------------------------
	    // Static Configuration
	    //--------------------------------------------------------
		String datasetEndDate = "2020-04-01";
		double volatilityCeiling = 4;
		double peRatioThreshold = 25;
	
		long startTime = System.currentTimeMillis();
		
		// The code submitted for the assessed exerise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef==null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
		
		// Get the location of the asset pricing data
		String pricesFile = System.getenv("BIGDATA_PRICES");
		if (pricesFile==null) pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries
		
		// Get the asset metadata
		String assetsFile = System.getenv("BIGDATA_ASSETS");
		if (assetsFile==null) assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries
		
		
    	//----------------------------------------
    	// Pre-provided code for loading the data 
    	//----------------------------------------
    	
    	// Create Datasets based on the input files
		
		// Load in the assets, this is a relatively small file
		Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
		//assetRows.printSchema();
		System.err.println(assetRows.first().toString());
		JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());
		
		// Load in the prices, this is a large file (not so much in data size, but in number of records)
    	Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
    	Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
    	Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert to Stock Price Objects
		
	
		AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling, peRatioThreshold);
		
		System.out.println(finalRanking.toString());
		
		System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");
		
		Thread.sleep(60000);
		
		// Close the spark session
		spark.close();
		
		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out!=null) resultsDIR = out;
		
		
		
		long endTime = System.currentTimeMillis();
		
		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(resultsDIR).getAbsolutePath()+"/SPARK.DONE")));
			
			Instant sinstant = Instant.ofEpochSecond( startTime/1000 );
			Date sdate = Date.from( sinstant );
			
			Instant einstant = Instant.ofEpochSecond( endTime/1000 );
			Date edate = Date.from( einstant );
			
			writer.write("StartTime:"+sdate.toGMTString()+'\n');
			writer.write("EndTime:"+edate.toGMTString()+'\n');
			writer.write("Seconds: "+((endTime-startTime)/1000)+'\n');
			writer.write('\n');
			writer.write(finalRanking.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}


    public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata, Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {
    	
    	//----------------------------------------
    	// Student's solution starts here
    	//----------------------------------------

		JavaPairRDD<String, StockPrice> pricesByTicker =
				prices.javaRDD()
						.mapToPair(price -> new Tuple2<>(price.getStockTicker(), price));
		JavaPairRDD<String, Iterable<StockPrice>> groupedPrices =
				pricesByTicker.groupByKey();
		System.out.println(groupedPrices.count());


		JavaPairRDD<String, List<Double>> sortedClosePrices = groupedPrices.mapValues(pricesIterable -> {

			List<StockPrice> priceList = new ArrayList<>();
			pricesIterable.forEach(priceList::add);
			priceList.sort(Comparator.comparing(
					p -> TimeUtil.fromDate(p.getYear(), p.getMonth(), p.getDay())
			));

			List<Double> closePrices = new ArrayList<>();
			for (StockPrice p : priceList) {
				closePrices.add(p.getClosePrice());
			}

			return closePrices;
		});
		JavaPairRDD<String, AssetFeatures> assetFeatures = sortedClosePrices.mapValues(closePrices -> {

			// need enough data for 1 year volatility
			if (closePrices.size() < 251) return null;

			List<Double> last251 = closePrices.subList(closePrices.size() - 251, closePrices.size());

			Returns returnsCalc = new Returns();
			double assetReturn = returnsCalc.calculate(5, closePrices);

			Volitility volCalc = new Volitility();
			double assetVol = volCalc.calculate(last251);

			AssetFeatures features = new AssetFeatures();
			features.setAssetReturn(assetReturn);
			features.setAssetVolitility(assetVol);

			return features;

		}).filter(x -> x._2 != null);

		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> joined =
				assetFeatures.join(assetMetadata);
		JavaPairRDD<String, AssetFeatures> withPERatio = joined.mapValues(tuple -> {

			AssetFeatures features = tuple._1;
			AssetMetadata metadata = tuple._2;

			double pe = metadata.getPriceEarningRatio();

			// filter missing / invalid PE
			if (pe == 0.0) return null;

			features.setPeRatio(pe);

			return features;

		}).filter(x -> x._2 != null);
		JavaPairRDD<String, AssetFeatures> filtered = withPERatio.filter(x ->
				x._2.getAssetVolitility() < volatilityCeiling &&
						x._2.getPeRatio() < peRatioThreshold
		);

		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> filteredWithMeta =
				joined.mapValues(tuple -> {

							AssetFeatures features = tuple._1;
							AssetMetadata metadata = tuple._2;

							double pe = metadata.getPriceEarningRatio();

							if (pe == 0.0) return null;

							features.setPeRatio(pe);

							return new Tuple2<>(features, metadata);

						})
						.filter(x ->
								x._2 != null &&
										x._2._1.getAssetVolitility() < volatilityCeiling &&
										x._2._1.getPeRatio() < peRatioThreshold
						);
		List<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>> top5 =
				filteredWithMeta.takeOrdered(
						5,
						new AssetReturnComparatorWithMeta()
				);
		Asset[] finalAssets = new Asset[5];

		for (int i = 0; i < top5.size(); i++) {

			String ticker = top5.get(i)._1;
			AssetFeatures features = top5.get(i)._2._1;
			AssetMetadata metadata = top5.get(i)._2._2;

			Asset asset = new Asset();

			asset.setTicker(ticker);
			asset.setName(metadata.getName());
			asset.setIndustry(metadata.getIndustry());
			asset.setSector(metadata.getSector());

			asset.setFeatures(features);

			finalAssets[i] = asset;
		}

		AssetRanking finalRanking = new AssetRanking();
		finalRanking.setAssetRanking(finalAssets);

    	// ...One of these is what your Spark program should collect
    	
    	return finalRanking;
    	
    	
    	
    }
	
}
