// map function for application to compute the mean and
// standard deviation for the number of bytes sent by
// each IP address in the IP address pair that defines
// a flow.  
import java.io.IOException;
import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Mapper;

public class ScoreStatsMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String comment = "";
    String score = "";
    String[] tokens = line.split(",");
	  // assumed score of 0 if not score
          if(tokens.length > 1){
                comment = tokens[0];
		score = tokens[1];
                if(comment.toLowerCase().contains("trump") &&comment.toLowerCase().contains("clinton")){
                comment = "Both";
                } else if(comment.toLowerCase().contains("trump")){
                        comment = "Trump";
                } else if(comment.toLowerCase().contains("clinton")){
                        comment = "Clinton";
                } else{
                        comment = "Neither";
                }
	   }else {
		comment = "bad data";
		score = "1";
	   }



        // output the key, value pairs where the key is an
        // IP address Flow-tuple and the value is a string giving
        // counts for the flow.
	 context.write(new Text(comment), new Text(score));
  }
}
