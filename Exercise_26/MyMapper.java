package it.polito.bigdata.hadoop.ex26;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
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
	
	private HashMap<String, String> dictionary;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		dictionary = new HashMap<>();
		@SuppressWarnings("deprecation")
		Path[] pathCachedFiles = context.getLocalCacheFiles();
		BufferedReader bReader = new BufferedReader(new FileReader(new File(pathCachedFiles[0].toString())));
		String line;
		String[] tmpArray;
		while ((line = bReader.readLine()) != null) {
			tmpArray = line.split("\t");
			dictionary.put(tmpArray[1], tmpArray[0]);
		}
		bReader.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split("\\s+");
		String tmp = "";
		for (String word : words) {
			if (dictionary.get(word) != null) {
				tmp += dictionary.get(word) + " ";
			}
		}
		tmp = tmp.substring(0, tmp.length()-1);
		context.write(new Text(tmp), NullWritable.get());
	}
}
