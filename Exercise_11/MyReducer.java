package it.polito.bigdata.hadoop.ex11;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 */
public class MyReducer extends Reducer<Text, MyCustom, Text, DoubleWritable> {

	@Override
	protected void reduce(
			Text key,
			Iterable<MyCustom> values,
			Context context)
			throws IOException, InterruptedException {

		float sum = 0;
		int count = 0;
		
		for (MyCustom value : values) {
			sum += value.getSum();
			count += value.getCount();
		}
		
		context.write(key, new DoubleWritable(sum/count));
	}
}
