// reducer function for application to compute the mean and
// standard deviation for the number of bytes sent by
// each IP address in the IP address pair that defines
// a flow.  Compute results only for flows that send 10
// or more ADUs in both directions.

import java.util.*;
import java.io.*;
import java.net.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.Reducer;

public class redditStatsReducer
  extends Reducer<Text, Text, Text, Text> {

  //define ArrayLists to cache byte counts from each of the ADUs
  //in a flow.  This local cacheing is necessary because the
  //Iterable provided by the MapReduce framework cannot be
  //iterated more than once. bytes1_2 caches byte counts sent
  //from IP address 1 to 2 and bytes2_1 caches those in the
  //opposite direction.

  private ArrayList<Double> lats = new ArrayList<Double>();

  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {
      long trump_count =0;
      long hilary_count= 0;      
      //double sum_latency = 0;  //total bytes sent IP address 1 to 2
      //double count_latency = 0; //ADU count IP address 1 to 2
      //double latency = 0; //starting latency; 

      // clear the cache for each new flow key
      //lats.clear();

      // iterate through all the values with a common key (FlowTuple)
      for ( Text value : values) {
          String line = value.toString();
          //String[] tokens = line.split(",");
         // latency = Double.valueOf(tokens[0]);
     // System.out.println(tokens.length);
	
      String str = line.toLowerCase();
      
      // str = str.replaceAll("\\s+", "");
       
      //int count=0;
      for (int i = str.indexOf("trump"); i >= 0; i = str.indexOf("trump", i + 1)){
   trump_count++;
      }
            for (int i = str.indexOf("hillary"); i >= 0; i = str.indexOf("hillary", i + 1)){
   hilary_count++;
      }
 
      }
      
 // if(tokens[2].toLowerCase().contains("trump"){

	//	trump_count++;
//	}
	/*
str = str.toLowerCase();
int count = 0;
for (int i = str.indexOf("the"); i >= 0; i = str.indexOf("the", i + 1))
    count++;
    */
	// only one of bytes1 and bytes2 will be non-zero
     /*
     	  if (latency > 0) {
     	  count_latency += 1;
             sum_latency += latency;
             lats.add(latency);  //cache byte count
          }
	  */
/*	 total_success +=  Long.parseLong(tokens[0]);
	 total_tries +=Long.parseLong(tokens[1]);
	 total_tries +=Long.parseLong(tokens[0]);

      }
      double percentage_success  = (double)total_success/total_tries*100;
   /*  
    // calculate the mean
    double mean_latency = sum_latency/count_latency;
    
    // calculate standard deviation
    double sumOfSquares = 0.0f;
   
    // compute sum of square differences from mean
    for (Double f : lats) {
        sumOfSquares += (f - mean_latency) * (f - mean_latency);
    }

    // compute the variance and take the square root to get standard deviation
    double stddev = (double) Math.sqrt(sumOfSquares / (count_latency - 1));
    
    // output byte mean and standard deviation for both IP addresses
    String FlowStats = Double.toString(mean_latency) + " " + Double.toString(stddev);
   */
    context.write(key, new Text("Trump: " + trump_count + " " + "Hillary: " + hilary_count ));
      
  }
}

