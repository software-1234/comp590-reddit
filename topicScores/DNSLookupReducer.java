
// reducer function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class DNSLookupReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		// iterate through all the values (count == 1) with a common key
		double total = 0;
		double count = 0;
		for (Text value : values) {
			total +=Double.parseDouble(value.toString());
			count++;
		}
		context.write(key, new Text("Average Score: "+ Double.toString(total / count)+"   Count: " + Double.toString(count)));

	}
}

