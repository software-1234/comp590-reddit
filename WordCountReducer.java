// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer
  extends Reducer<Text, IntWritable, Text, LongWritable> {
  @Override
  public void reduce(Text key, Iterable<IntWritable> values,
      Context context)
      throws IOException, InterruptedException {

      long count = 0;
      // iterate through all the values (count == 1) with a common key
      for (IntWritable value : values) {
          count = count + value.get();
      }
    if (count > 20) {
    context.write(key, new LongWritable(count));
    }
  }
}
