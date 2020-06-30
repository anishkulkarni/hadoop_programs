import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Weather
{
	//mapper class
	public static class TemperatureMapper extends Mapper<Object,Text,Text,IntWritable>
	{
		private static final int MISSING= 9999;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			//tokenize and extract field
			String year = line.substring(15, 19);
			int airTemperature;
			if (line.charAt(87)=='+')
			{
				//positive
				airTemperature = Integer.parseInt(line.substring(88, 92));
			}
			else
			{
				//negative
				airTemperature = Integer.parseInt(line.substring(87, 92));
			}
			String quality= line.substring(92, 93);
			if (airTemperature != MISSING && quality.matches("[01459]"))
			{
				//output of map
				context.write(new Text(year), new IntWritable(airTemperature));
			}
		}
	}
	//reducer class
	public static class TemperatureReducer extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		static int min=9999, max=0;
		static Text min_year = new Text(), max_year = new Text();
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException
		{
			int average = 0;
			int count = 0;
			String year = key.toString();
			//all input from map - list
			for(IntWritable value:values)
			{
				average = average + value.get();
				count++;
			}
			//calculate average
			average = average / count;
			//find min and max average
			if(average < min)
			{
				min = average;
				min_year.set(key);
			}
			if(average > max)
			{
				max = average;
				max_year.set(key);
			}
		}
		public void cleanup(Context context) throws IOException,InterruptedException
		{
			//write result
			context.write(min_year, new IntWritable(min));
			context.write(max_year, new IntWritable(max));
		}
	}

	public static void main(String[] args) throws Exception
	{
		//configure job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "weather");
		job.setJarByClass(Weather.class);
		//mapper class
		job.setMapperClass(TemperatureMapper.class);
		//combiner class
		job.setCombinerClass(TemperatureReducer.class);
		//reducer class
		job.setReducerClass(TemperatureReducer.class);
		//output key type
		job.setOutputKeyClass(Text.class);
		//output value type
		job.setOutputValueClass(IntWritable.class);
		//input path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//output path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
 	}
}
