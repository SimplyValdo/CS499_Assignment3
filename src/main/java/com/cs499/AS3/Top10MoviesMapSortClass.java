/*Ubaldo Jimenez Prieto
 * February 28, 2016
 * Assignment # 3
 * CS499
 */

package com.cs499.AS3;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10MoviesMapSortClass extends Mapper<LongWritable, Text, DoubleWritable, IntWritable> {

	private IntWritable rating = new IntWritable();
	private DoubleWritable movieID = new DoubleWritable();

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, DoubleWritable, IntWritable>.Context context)
			throws IOException, InterruptedException 
	{		
		String[] line = value.toString().split("\t");
		movieID.set(Double.parseDouble(line[1]));
		rating.set(Integer.parseInt(line[0]));
		context.write(movieID, rating);
	}
}