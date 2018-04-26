/**
 * 
 */
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
 * Apr 24, 2018
 */
public class MyReducer extends Reducer<Text, MyCustomType, Text, DoubleWritable> {

	@Override
	protected void reduce(Text key, Iterable<MyCustomType> values,
			Reducer<Text, MyCustomType, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		
		int count = 0;
		double sum = 0;
		
		for (MyCustomType value : values) {
			count += value.getCount();
			sum += value.getSum();
		}
		
		context.write(key, new DoubleWritable(sum/count));
	}
}
