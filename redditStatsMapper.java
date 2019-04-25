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

public class redditStatsMapper
  extends Mapper<LongWritable, Text, Text, Text> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
   String keye = key.toString();
    //line = line.toLowerCase();
    
    String[] tokens = line.split(",");
    String comment = new String();

    
   
        // carrier
	
        comment = tokens[0];
	if(!tokens[2].equals("created_utc")){
	int day = Integer.parseInt(tokens[2])/86400;
	//carrier = carrier.toLowerCase();
	
	// Taking from join data, so add 7 to original, (number of fields plus additional double comma minus the addition unit_id that was in webget)
	// double ttfb_avg_time = Double.parseDouble(tokens[26]);
	//int  success = Integer.parseInt(tokens[32]);
        //String throughput = Double.toString(bytes_sec);
        //String he = success + " " + tokens[33] + " " + tokens[31];
	//if(!type.equals("remove") && !type.equals("misc")){


	context.write(new Text(day+""), new Text(comment));
	}
       	// }
//	}
  } 
}
