package it.polito.bigdata.hadoop.ex5;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 */
public class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	protected void reduce(
			Text key,
			Iterable<DoubleWritable> values,
			Context context
		) throws IOException, InterruptedException {

		int count = 0;
		double sum = 0;

		for (DoubleWritable value : values) {
			count++;
			sum += value.get();
		}

		context.write(key, new DoubleWritable(sum/count));
	}
}
