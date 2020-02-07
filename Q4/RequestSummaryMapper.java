package ques4;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mortbay.log.Log;
import java.io.IOException;

public class RequestSummaryMapper extends Mapper<LongWritable, Text, Text, Summary> {

	public String getStringBetweenTwoChars(String input, String startChar, String endChar) {
		try {
			int start = input.indexOf(startChar);
			if (start != -1) {
				int end = input.indexOf(endChar, start + startChar.length());
				if (end != -1) {
					return input.substring(start + startChar.length(), end);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return input; // return null; || return "" ;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String timeStr = StringUtils.substringBetween(line, "[", "]");
		Log.debug("msg", timeStr);
		String month = timeStr.substring(3, 5);
		String year = timeStr.substring(7, 10);
		String combined = month + "-" + year;
		Log.debug("combined", combined);
		Summary summary = new Summary();
		summary.setRequests(1);
		int bytes = 0;
		if (line.contains("GET")) {
			int index = line.indexOf("1.1");
			String temp = "";
			index += 9;
			temp += line.charAt(index);
			while (line.charAt(index++) != '\n') {
				temp += line.charAt(index);
			}
			bytes = Integer.valueOf(temp);
		}
		summary.setDownloadSize(bytes);
		context.write(new Text(combined), summary);
	}

}