package it.polito.bigdata.hadoop.ex21;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
	
	private ArrayList<String> stopwords;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		stopwords = new ArrayList<>();
		@SuppressWarnings("deprecation")
		Path[] pathCachedFiles = context.getLocalCacheFiles();
		BufferedReader bReader = new BufferedReader(new FileReader(new File(pathCachedFiles[0].toString())));
		String line;
		while ((line = bReader.readLine()) != null) {
			stopwords.add(line);
		}
		bReader.close();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split("\\s+");
		String tmp = "";
		for (String word : words) {
			if (!stopwords.contains(word)) {
				tmp += word + " ";
			}
		}
		tmp = tmp.substring(0, tmp.length()-1);
		context.write(new Text(tmp), NullWritable.get());
	}
}
