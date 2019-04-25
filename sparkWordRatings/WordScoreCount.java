// Spark implementation of job to count the number of times each
// unique IP address 4-tuple appears in an adudump file.
//

import scala.Tuple2;

import org.apache.spark.SparkContext.*;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import java.util.*;
import java.io.*;
import java.text.*;

public final class WordScoreCount {

	// The first argument to the main function is the HDFS input file name
	// (specified as a parameter to the spark-submit command). The
	// second argument is the HDFS output directory name (must not exist
	// when the program is run).

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: WordScoreCount <input file> <output file>");
			System.exit(1);
		}

		// Create session context for executing the job
		SparkSession spark = SparkSession.builder().appName("WordScoreCount").getOrCreate();

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

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterator<String> call(String s) {
				String[] tokens = s.split(",");
				
				if (tokens.length > 0) {
					tokens[0] = tokens[0].replace(".", "");
					tokens[0] = tokens[0].replace("!", "");
					tokens[0] = tokens[0].replace("?", "");
					tokens[0] = tokens[0].replace("\"", "");
					String[] comment = tokens[0].split("\\s");
					HashSet<String> output = new HashSet<String>();
					if (comment.length > 0) {
						for (String word : comment) {
							if(word.length() > 0 && tokens[1].length() > 0) {
								word = word.toLowerCase();
								output.add(word + " " + tokens[1]);
							}
						}
						if(output.size() > 0)  {
							String[] out = new String[output.size()];
							return Arrays.asList(output.toArray(out)).iterator();
						}
					}
				}
				String[] output = {"Null 0"};
				return Arrays.asList(output).iterator();
			}
		});

		// Create a PairRDD of <Key, Value> pairs from an RDD. The input RDD
		// contains strings and the output pairs are <String, Integer>.
		// The Tuple2 object is used to return the pair. mapToPair applies
		// the provided function to each element in the input RDD.
		// The PairFunction will be called for each string element (IP address)
		// in the 'words' RDD. It will return the IP address as the first
		// element (key) and 1 for the second (value).

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {
				String[] tokens = s.split(" ");
				int x = 0;
				try {
					x = Integer.parseInt(tokens[1]);
				} catch (Exception e) {
					x = 0;
					return new Tuple2<>("Null", x);
				}
				return new Tuple2<>(tokens[0], x);
			}
		});

		// Create a PairRDD where each element is one of the keys from a PairRDD and
		// a value which results from invoking the supplied function on all the
		// values that have the same key. In this case, the value returned
		// from the jth invocation is given as an input parameter to the j+1
		// invocation so a cumulative value is produced.

		// The Function2 reducer is similar to the reducer in Mapreduce.
		// It is called for each value associated with the same key.
		// The two inputs are the value from the (K,V) pair in the RDD
		// and the result from the previous invocation (0 for the first).
		// The sum is stored in the result PairRDD with the associated key.

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// Format the 'counts' PairRDD and write to a HDFS output directory
		// Because parts of the RDD may exist on different machines, there will
		// usually be more than one file in the directory (as in MapReduce).
		// Use 'hdfs dfs -getmerge' as before.

		counts.saveAsTextFile(args[1]);

		spark.stop();
	}
}

