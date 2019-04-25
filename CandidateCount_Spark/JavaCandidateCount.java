// Spark implementation of job to count the number of comments 
// that containt Trump, Clinton, Both, or neither.
//

/**
 * Author: Gabi Stein
 * ONYEN: ghstein
 * UNC Honor Pledge: I certify that no unauthorized assistance has been received   
 * or given in the completion of this work. I certify that I understand and 
 * could now rewrite on my own, without assistance from course staff,  
 * the problem set code I am submitting.
 */


import scala.Tuple2;

import org.apache.spark.SparkContext.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;


import java.util.*;
import java.io.*;
import java.text.*;

public final class JavaCandidateCount {

  // The first argument to the main function is the HDFS input file name
  // (specified as a parameter to the spark-submit command).  The
  // second argument is the HDFS output directory name (must not exist
  // when the program is run).

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: CandidateCount <input file> <output file>");
      System.exit(1);
    }

    // Create session context for executing the job
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaCandidateCount")
      .getOrCreate();

    // Create a JavaRDD of strings; each string is a line read from
    // a text file.
    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();


    // The flatMap operation applies the provided function to each
    // element in the input RDD and produces a new RDD consisting of
    // the strings returned from each invocation of the function.
    // The strings are returned to flatMap as an iterator over a
    // list of strings (string array to string list with iterator).
    //
    // The RDD 'words' will have an entry for each IP address that
    // appears in the input HDFS file (most addresses will appear
    // more than once and be repeated in 'words').

    JavaRDD<String> words = lines.flatMap(
      new FlatMapFunction<String, String>() {
        @Override
        public Iterator<String> call(String s) {
              String[] tokens = s.split(",");
              String comment = "";
              if(tokens.length > 0){
                comment = tokens[0];
  
                if(comment.toLowerCase().contains("trump") &&comment.toLowerCase().contains("clinton")){
                comment = "Both";
                } else if(comment.toLowerCase().contains("trump")){
                        comment = "Trump";
                } else if(comment.toLowerCase().contains("clinton")){
                        comment = "Clinton";
                } else{
                        comment = "Neither";
                }
             }else{
                comment = "Neither";
             }
                
                String[] comments = {comment};
  
                return Arrays.asList(comments).iterator();
            }
              
        }
    );


    //Create a PairRDD of <Key, Value> pairs from an RDD.  The input RDD
    //contains strings and the output pairs are <String, Integer>.
    //The Tuple2 object is used to return the pair.  mapToPair applies
    //the provided function to each element in the input RDD.
    //The PairFunction will be called for each string element (IP address)
    //in the 'words' RDD.  It will return the IP address as the first
    //element (key) and 1 for the second (value).

    JavaPairRDD<String, Integer> ones = words.mapToPair(
      new PairFunction<String, String, Integer>() {
        public Tuple2<String, Integer> call(String s) {
            return new Tuple2<>(s, 1); //key = IP, value = 1
        }
      });

    //Create a PairRDD where each element is one of the keys from a PairRDD and
    //a value which results from invoking the supplied function on all the
    //values that have the same key.  In this case, the value returned
    //from the jth invocation is given as an input parameter to the j+1
    //invocation so a cumulative value is produced.

    //The Function2 reducer is similar to the reducer in Mapreduce.
    //It is called for each value associated with the same key.
    //The two inputs are the value from the (K,V) pair in the RDD
    //and the result from the previous invocation (0 for the first).
    //The sum is stored in the result PairRDD with the associated key.

    JavaPairRDD<String, Integer> counts = ones.reduceByKey(
      new Function2<Integer, Integer, Integer>() {
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

    //Format the 'counts' PairRDD and write to a HDFS output directory
    //Because parts of the RDD may exist on different machines, there will
    //usually be more than one file in the directory (as in MapReduce).
    //Use 'hdfs dfs -getmerge' as before.

    counts.saveAsTextFile(args[1]);

    spark.stop();
  }
}

