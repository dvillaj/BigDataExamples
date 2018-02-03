import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		if(line.length() > 0) {
			String[] words = line.split("\\W+");
			if(words.length > 0) {
				for(String word : words) {
					context.write(new Text(word), new IntWritable(1));
				}
			}
		}
	}
	
	

}
