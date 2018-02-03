import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class CountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text word, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		
		long total = 0;
		for(IntWritable value : values) {
			total += value.get();
		}
		context.write(word, new LongWritable(total));
	}

}
