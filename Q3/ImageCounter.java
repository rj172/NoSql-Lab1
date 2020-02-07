

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class ImageCounter {

	
	public static class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String imageType = "";
		if (line.contains("GET") && line.contains("images")) {
			if (line.contains(".jpg")) {
				imageType = "JPG";
			} else if (line.contains(".gif")) {
				imageType = "GIF";
			} else {
				imageType = "OTHERS";
			}
		}
		context.write(new Text(imageType), new IntWritable(1));
	}
	}


	public static class ImageCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		int counter = 0;
		for (IntWritable value : values) {
			counter += value.get();
		}
		context.write(key, new IntWritable(counter));
	}
	}


	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: ImageCounter <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(ImageCounter.class);
		job.setJobName("Image Counter");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(ImageCounterMapper.class);
		job.setReducerClass(ImageCounterReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
