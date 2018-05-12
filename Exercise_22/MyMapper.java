package it.polito.bigdata.hadoop.ex22;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(",");
		if (words[0].equalsIgnoreCase(context.getConfiguration().get("username"))) {
			context.write(NullWritable.get(), new Text(words[1]));
		} else if (words[1].equalsIgnoreCase(context.getConfiguration().get("username"))) {
			context.write(NullWritable.get(), new Text(words[0]));
		}
	}

}
