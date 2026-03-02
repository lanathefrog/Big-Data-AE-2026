package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;

import bigdata.objects.*;
import bigdata.transformations.comparators.AssetReturnComparatorWithMeta;
import bigdata.transformations.filters.AssetFilterWithMeta;
import bigdata.transformations.filters.NonNullAssetFeaturesFilter;
import bigdata.transformations.filters.PriceDateFilter;
import bigdata.transformations.maps.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.pairing.AssetMetadataPairing;
import scala.Tuple2;
import bigdata.util.TimeUtil;
import bigdata.transformations.maps.CalculateAssetFeatures;

import java.util.List;

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


		// Define a time window of 730 days ending at the dataset end date
		// This guarantees us at least 251 trading days in the window, which is the minimum required to calculate the volatility
		Instant endDate = TimeUtil.fromDate(datasetEndDate);
		Instant startDate = endDate.minusSeconds(730L * 24 * 60 * 60);


		// Filter the prices to only include those within the specified date range
		JavaRDD<StockPrice> filteredPrices =
				prices.javaRDD().filter(new PriceDateFilter(startDate, endDate));

		// Convert the filtered prices into pairs of (stock ticker, (date, close price))
		// Only the needed information is kept to save memory and speed up the processing
		JavaPairRDD<String, Tuple2<Instant, Double>> pricesByTicker =
				filteredPrices.mapToPair(new PriceToDateClosePair());


		// Group all price records for each asset
		JavaPairRDD<String, ArrayList<Tuple2<Instant, Double>>> groupedPrices =
				pricesByTicker.aggregateByKey(
						new ArrayList<>(),
						(list, value) -> { list.add(value); return list; },
						(l1, l2) -> { l1.addAll(l2); return l1; }
				);
		// Sort the price records for each asset by date
		// Extract the close prices into a list
		JavaPairRDD<String, List<Double>> sortedClosePrices =
				groupedPrices.mapValues(new SortAndExtractClosePricesFromTuple());


		// For each asset, calculate the return and volatility
		// Assets that have insufficient price history will be filtered out in the next step
		JavaPairRDD<String, AssetFeatures> assetFeatures =
				sortedClosePrices
						.mapValues(new CalculateAssetFeatures())
						.filter(new NonNullAssetFeaturesFilter());

		// Join calculated features and metadata for each asset
		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> joined =
				assetFeatures.join(assetMetadata);

		// Filter assets based on the specified volatility and PE ratio thresholds
		JavaPairRDD<String, Tuple2<AssetFeatures, AssetMetadata>> filtered =
				joined.filter(new AssetFilterWithMeta(volatilityCeiling, peRatioThreshold));

		// Sort the remaining assets by return and take the top 5
		List<Tuple2<String, Tuple2<AssetFeatures, AssetMetadata>>> top5 =
				filtered.takeOrdered(5, new AssetReturnComparatorWithMeta());

		// Create the final Asset objects for the top 5 assets
		Asset[] finalAssets = new Asset[5];

		for (int i = 0; i < top5.size(); i++) {

			// Extract the ticker, features, and metadata for each of the top 5 assets
			String ticker = top5.get(i)._1;
			AssetFeatures features = top5.get(i)._2._1;
			AssetMetadata metadata = top5.get(i)._2._2;

			// Create an Asset object and populate it with the extracted information
			Asset asset = new Asset();

			asset.setTicker(ticker);
			asset.setName(metadata.getName());
			asset.setIndustry(metadata.getIndustry());
			asset.setSector(metadata.getSector());
			asset.setFeatures(features);

			finalAssets[i] = asset;
		}

		// Create the final AssetRanking object and set the ranked assets
		AssetRanking finalRanking = new AssetRanking();
		finalRanking.setAssetRanking(finalAssets);

    	return finalRanking;

		// Thank you for your attention :)
    	
    	
    	
    }
	
}
