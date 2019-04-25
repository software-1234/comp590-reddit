// map function for application to count the number of
// times each unique IP address 4-tuple appears in an
// adudump file.
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class CandidateCountMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    if(tokens.length > 0){
    // get the comment fields 
    String comment = tokens[0];
    if(comment.length() > 0){
	if(comment.toLowerCase().contains("trump") && comment.toLowerCase().contains("clinton")) {
		comment = "Both";
	} else if(comment.toLowerCase().contains("trump")){
		comment = "Trump";
	} else if(comment.toLowerCase().contains("clinton")){
		comment = "Clinton";
	} else{
		comment = "Neither";
	}
      }
      
        // output the key, value pairs where the key is an
        // IP address 4-tuple and the value is 1 (count)
        context.write(new Text(comment), new IntWritable(1));
  }
	}
}

