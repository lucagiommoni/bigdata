package it.polito.bigdata.hadoop.ex10;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyReducer extends Reducer<NullWritable, NullWritable, NullWritable, NullWritable> {

	@Override
	protected void reduce(
			NullWritable key, 
			Iterable<NullWritable> values,
			Reducer<NullWritable, NullWritable, NullWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(NullWritable.get(), NullWritable.get());
	}

}

