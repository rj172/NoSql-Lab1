

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class TimeStamp {

	public static class TimeStampMapper extends Mapper<LongWritable, Text, Text, Text> {

	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String timeStamp = "";
		String url = "";

		if (line.contains("404")) {
			int i = 11;
			while (line.charAt(i) != '[') {
				i++;
			}
			i++;

			while (line.charAt(i) != ']') {
				timeStamp = timeStamp + line.charAt(i);
				i++;
			}
			int count = 0;
			while (count != 3) {
				if (line.charAt(i) == '"') {
					count++;
				}
				i++;
			}
			while (line.charAt(i) != '"') {
				url = url + line.charAt(i);
				i++;
			}
			context.write(new Text(timeStamp), new Text(url));
		}
	}
}
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: ImageCounter <input path> <output path>");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJarByClass(TimeStamp.class);
		job.setJobName("Image Counter");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TimeStampMapper.class);
		//job.setReducerClass(ImageCounterReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
