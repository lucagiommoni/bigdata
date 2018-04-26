package it.polito.bigdata.hadoop.ex6;

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
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		String[] tmp;
		double max = Double.MIN_VALUE;
		double min = Double.MAX_VALUE;
		double tmpMin;
		double tmpMax;
		
		for (Text value : values) {
			tmp = value.toString().split(",");
			tmpMin = Double.parseDouble(tmp[0]);
			tmpMax = Double.parseDouble(tmp[1]);
			if (tmpMin < min) min = tmpMin;			
			if (tmpMax > max) max = tmpMax;
		}
		
		context.write(key, new Text("max=" + max + "_min=" + min));
	}

}
