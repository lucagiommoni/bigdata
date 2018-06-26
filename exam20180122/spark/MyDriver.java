package it.polito.bigdata.spark.exam20180122;

import org.apache.spark.api.java.*;

import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;

public class MyDriver {

	public static void main(String[] args) {

		String booksInput = args[0];
		String purchasesInput = args[1];
		String outputPath1 = args[2];
		String outputPath2 = args[3];

		SparkConf conf = new SparkConf().setAppName("exam20180122_v2");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// ----------------------------------------------------------------------
		// PART A
		// ----------------------------------------------------------------------


		JavaPairRDD<String, String> rec2017 = sc.textFile(purchasesInput)
									.filter(line ->
									{
										return line.split(",")[2].split("_")[0].substring(0, 4).equals("2017");
									}).
									map(line ->
									{
										String fields[] = line.split(",");
										return fields[1]+","+fields[3];
									})
									.distinct()
									.mapToPair(line ->
									{
										String fields[] = line.split(",");
										return new Tuple2<String, String>(fields[0], fields[1]);
									});

		JavaPairRDD<String, String> minPrice = rec2017.reduceByKey((v1, v2) -> {return Double.parseDouble(v1) < Double.parseDouble(v2) ? v1 : v2;});

		JavaPairRDD<String, String> maxPrice = rec2017.reduceByKey((v1, v2) -> {return Double.parseDouble(v1) > Double.parseDouble(v2) ? v1 : v2;});

		minPrice.union(maxPrice)
			    .reduceByKey((v1, v2) -> {return (Math.abs(Double.parseDouble(v1) - Double.parseDouble(v2)) >= 15) ? (Double.parseDouble(v1) > Double.parseDouble(v2) ? (v1 + "," + v2) : (v2 + "," + v1)) : null;})
			    .filter(tuple -> {return tuple._2() != null;})
			    .sortByKey(true)
			    .saveAsTextFile(outputPath1);

		// ----------------------------------------------------------------------
		// PART B
		// ----------------------------------------------------------------------

		JavaRDD<String> books = sc.textFile(booksInput);
		JavaRDD<String> purchases = sc.textFile(purchasesInput).filter(line -> {return line.split(",")[2].split("_")[0].substring(0, 4).equals("2017");});

		List<String> notSoldBID = books.map(line-> {return line.split(",")[0];}).subtract(purchases.map(line->{return line.split(",")[1];})).collect();

		HashMap<String, Long> countGenres = new HashMap<>(books.filter(line->{return notSoldBID.contains(line.split(",")[0]);}).map(line->{return line.split(",")[2];}).countByValue());
		HashMap<String, Double> percGenres  = new HashMap<>();

		double total = 0;

		for (String genre : countGenres.keySet()) total += countGenres.get(genre);

		for (String genre : countGenres.keySet()) {
			double perc = (double) ((int) (countGenres.get(genre) / total * 100)) / 100;
			percGenres.put(genre, perc);
		}


		books.map(line->{return line.split(",")[2];})
			 .distinct()
			 .sortBy((line)->{return line;}, true, 0)
			 .map(line->{return new String(line + "," + (percGenres.get(line) == null ? 0L : percGenres.get(line)));})
			 .saveAsTextFile(outputPath2);

		sc.close();
	}
}
