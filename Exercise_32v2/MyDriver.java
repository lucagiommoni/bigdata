package it.polito.bigdata.spark.ex32;

import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.max;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * MyDriver.java
 *
 * @version 2.0
 *
 * Jun 5, 2018
 *
 * Extract max value from csv using dataframe
 */
public class MyDriver {

	public static void main(String[] args) {

		String inputPath = args[0];

		// Create new Spark session and assign a name to it
		SparkSession ss = SparkSession.builder().appName("Spark ex32").getOrCreate();

		// Create a new DataFrame from csv file
		Dataset<Row> dfRead = ss.read().format("csv").option("header", false).option("inferSchema", true).load(inputPath);

		// Apply an aggregate function over the third column
		// Columns start with _c0
		// The result is a DataSet with one Row object only
		Dataset<Row> maxValDF = dfRead.agg(max("_c2"));

		// Extract the row using first() method
		Row maxRow = maxValDF.first();

		// Extract and convert to double the value of column max(_c2)
		Double max = (Double) maxRow.getAs("max(_c2)");

		System.out.println(max);

		ss.stop();
	}
}
