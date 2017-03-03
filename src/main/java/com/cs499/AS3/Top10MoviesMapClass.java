/*Ubaldo Jimenez Prieto
 * February 28, 2016
 * Assignment # 3
 * CS499
 */

package com.cs499.AS3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10MoviesMapClass extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	private IntWritable movieID = new IntWritable();
	private DoubleWritable rating = new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context)
			throws IOException, InterruptedException 
	{		
		String[] line = value.toString().split(",");
		movieID.set(Integer.parseInt(line[0]));
		rating.set(Double.parseDouble(line[2]));
		context.write(movieID, rating);
		
	}
}