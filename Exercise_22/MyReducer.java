package it.polito.bigdata.hadoop.ex22;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 */
public class MyReducer extends Reducer<NullWritable, Text, Text, NullWritable> {


	@Override
	protected void reduce(
			NullWritable key,
			Iterable<Text> values,
			Context context)
			throws IOException, InterruptedException {

		LinkedList<String> friends = new LinkedList<>();
		
		for (Text value : values) {
			if (!friends.contains(value.toString())) {
				friends.add(value.toString());
			}
		}
		
		String tmp = "";
		
		for (String s : friends) {
			tmp += s + ",";
		}
		
		tmp = tmp.substring(0, tmp.length()-1);
		
		context.write(new Text(tmp), NullWritable.get());
	}
}
