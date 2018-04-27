package it.polito.bigdata.hadoop.ex8;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyReducer.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyReducer extends Reducer<Text, MyCustom, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<MyCustom> values,
			Reducer<Text, MyCustom, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
		
		HashMap<String, Integer> monthIncome = new HashMap<String, Integer>();
		Integer tmp;
		int monthCount = 0;
		int sumIncome = 0;
		
		for (MyCustom value : values) {
			if((tmp = monthIncome.get(value.getMonth())) == null) {
				monthIncome.put(value.getMonth(), value.getIncome());
				monthCount++;
			} else {
				monthIncome.put(value.getMonth(), value.getIncome() + tmp);
			}			
		}
		
		for (Entry<String, Integer> entry : monthIncome.entrySet()) {
			sumIncome += entry.getValue();
			context.write(new Text(key + "-" + entry.getKey()), new FloatWritable(entry.getValue()));
		}
		context.write(key, new FloatWritable(sumIncome/monthCount));
	}	
}
