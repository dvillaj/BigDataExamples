// cc MaxTemperatureReducer Reducer for maximum temperature example
// vv MaxTemperatureReducer

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MeanTemperatureReducer
  extends Reducer<Text, IntWritable, Text, FloatWritable> {
  
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {
    
    int sumValue = 0;
    int count = 0;
    for (IntWritable value : values) {
      sumValue += value.get();
      count += 1;
    }

    if (count > 0) {
      context.write(key, new FloatWritable((float) sumValue / count));
    }
  }
}
// ^^ MaxTemperatureReducer
