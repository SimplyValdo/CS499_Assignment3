/*Ubaldo Jimenez Prieto
 * February 28, 2016
 * Assignment # 3
 * CS499
 */

package com.cs499.AS3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10MoviesReduceClass extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> 
{

	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
			Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException 
	{
		int counter = 0;
		double sum = 0;
		Iterator<DoubleWritable> i = values.iterator();
		
		while(i.hasNext()) 
		{
			double count = i.next().get();
			sum += count;
			
			counter++;
			System.out.println(key.toString() + "-" + sum + "-" + counter);
		}
		
		System.out.println("sum" + sum);
		System.out.println("counter" + counter);
		context.write(key, new DoubleWritable(sum/counter));
	}

}