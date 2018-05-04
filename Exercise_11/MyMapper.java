package it.polito.bigdata.hadoop.ex11;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * MyMapper.java
 *
 * @version 1.0
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, MyCustom> {
	
	private HashMap<String, MyCustom> map;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		map = new HashMap<>();
	}	

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		for (Entry<String, MyCustom> entry : map.entrySet()) {
			context.write(new Text(entry.getKey()), entry.getValue());
		}
	}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String[] tmp = value.toString().toLowerCase().split(",");
		MyCustom myCustom;
		
		if (map.get(tmp[0]) == null) {
			myCustom = new MyCustom();
			myCustom.setCount(1);
			myCustom.setSum(Double.parseDouble(tmp[2]));
			map.put(tmp[0], myCustom);
		} else {
			myCustom = map.get(tmp[0]);
			myCustom.setCount(myCustom.getCount()+1);
			myCustom.setSum(myCustom.getSum() + Float.parseFloat(tmp[2]));
			map.put(tmp[0], myCustom);
		}
	}

}
