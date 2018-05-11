package it.polito.bigdata.hadoop.ex19;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MapperEx12.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	private Float MAX_THREASHOLD;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		this.MAX_THREASHOLD = Float.parseFloat(context.getConfiguration().get("MAX_THRESHOLD"));
	}

	@Override
	protected void map(
			LongWritable key, 
			Text value, 
			Context context
			) throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		Float tmp = Float.parseFloat(fields[3]);
		if (tmp < this.MAX_THREASHOLD) {
			context.write(value, NullWritable.get());
		}
	}
}
