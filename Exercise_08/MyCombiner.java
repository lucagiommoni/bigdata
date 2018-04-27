package it.polito.bigdata.hadoop.ex8;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * MyCombiner.java
 *
 * @version 1.0
 *
 * Apr 24, 2018
 */
public class MyCombiner extends Reducer<Text, MyCustom, Text, MyCustom> {

	@Override
	protected void reduce(Text key, Iterable<MyCustom> values, Reducer<Text, MyCustom, Text, MyCustom>.Context context)
			throws IOException, InterruptedException {
		
		HashMap<String, Integer> monthIncome = new HashMap<String, Integer>();
		Integer tmp;
		MyCustom myCustom;
		
		for (MyCustom value : values) {
			if((tmp = monthIncome.get(value.getMonth())) == null) {
				monthIncome.put(value.getMonth(), value.getIncome());
			} else {
				monthIncome.put(value.getMonth(), value.getIncome() + tmp);
			}
			
		}
		
		for (Entry<String, Integer> entry : monthIncome.entrySet()) {
			myCustom = new MyCustom();
			myCustom.setMonth(entry.getKey());
			myCustom.setIncome(entry.getValue());
			context.write(key, myCustom);
		}
	}
}
