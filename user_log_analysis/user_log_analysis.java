/*
Dataset Description:
IP address - IP address of internet users
MAC address - MAC address of internet users
Login time - login time of user
Logout time - logout time of user
*/

//Import all packages required for Hadoop, I/O and Exceptions.
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Userlog
{
	//Mapper Class
	public static class UserLogMapper extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final int MISSING = 9999;//missing value
		//Mapper function
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			Date login, logout;
			long dur;
			int duration;
			String line = value.toString();//Extract line as string
			String[] tokens = line.split(",");//Split line using , as delimiter
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			login = formatter1.parse(tokens[5]); //Parse login time and date acc to SimpleDateFormat
			logout = formatter1.parse(tokens[7]); //Parse logout time and date
			dur = (logout.getTime()-login.getTime())/1000; //calculate login duration, convert to seconds
			duration = (int)dur;
			//write ip and login duration as output for reducer
			Text outputKey = new Text(tokens[1]);
			context.write(outputKey, new IntWritable(duration));
		}
	}
	//Reducer Class
	public static class UserLogReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		int max = 0;
		Text maxLogin = new Text();
		//Reducer function
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
		{
			int sum = 0;
			for(IntWritable value : values)

			{
				sum += value.get(); //calculate total login duration
			}
			if(sum > max) // if login duration is greater than previous max
			{
				max = sum;
				maxLogin.set(key);
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			context.write(maxLogin, new IntWritable(max)); //print user with max login duration
		}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "User Log");//Get new job named &#39;User Log&#39;
		job.setJarByClass(Userlog.class);//Set main class
		FileInputFormat.addInputPath(job, new Path(args[0]));//set input file path
		FileOutputFormat.setOutputPath(job, new Path(args[1]));//set output file path
		job.setMapperClass(UserLogMapper.class);//set mapper class
		job.setReducerClass(UserLogReducer.class);//set reducer class
		job.setOutputKeyClass(Text.class);//set key class of output
		job.setOutputValueClass(IntWritable.class);//set value class of output
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}