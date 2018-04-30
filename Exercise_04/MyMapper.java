package it.polito.bigdata.hadoop.ex4;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyMapper extends Mapper<Text, Text, Text, Text> {
	
	private final float THRESHOLD = 50; 

	@Override
	protected void map(
			Text key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		float tmpVal = Float.parseFloat(value.toString());
		if (tmpVal > THRESHOLD) {
			String[] tmp = key.toString().toLowerCase().split(",");
			context.write(new Text(tmp[0]), new Text(tmp[1]));
		}
			
	}

}
