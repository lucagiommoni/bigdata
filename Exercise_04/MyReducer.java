package it.polito.bigdata.hadoop.ex4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	protected void reduce(
			Text key,
			Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {
		String tmpList = "";
		for (Text value : values) {
			tmpList += value.toString() + ",";
		}
		tmpList = tmpList.substring(0, tmpList.length()-1);
		context.write(key, new Text(tmpList));
	}
}
