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

public class ScoreStatsReducer
  extends Reducer<Text, Text, Text, Text> {

  //define ArrayLists to cache byte counts from each of the ADUs
  //in a flow.  This local cacheing is necessary because the
  //Iterable provided by the MapReduce framework cannot be
  //iterated more than once. bytes1_2 caches byte counts sent
  //from IP address 1 to 2 and bytes2_1 caches those in the
  //opposite direction.

  
  // private ArrayList<Float> bytes2_1 = new ArrayList<Float>();

  @Override
  public void reduce(Text key, Iterable<Text> values,
      Context context)
      throws IOException, InterruptedException {

      double sum = 0;  //total bytes sent IP address 1 to 2
      // float sum2_1 = 0;  //total bytes sent IP address 2 to 1
      double count = 0; //ADU count IP address 1 to 2
      // float count2_1 = 0; //ADU count IP address 1 to 2

            // bytes2_1.clear();

      // iterate through all the values with a common key (FlowTuple)
       
	for ( Text value : values) {
	  try{
          String line = value.toString();
          // String[] tokens = line.split("\\s");
		// shouldn't all pieces have two parts???? how is this index out of bounds??
          sum += Double.parseDouble(line);
          count += 1;
	}catch(Exception e){}



      }
          // calculate the mean
          double mean = sum/count;

          String FlowStats = Double.toString(mean);
          context.write(key, new Text(FlowStats));
      }
  }


