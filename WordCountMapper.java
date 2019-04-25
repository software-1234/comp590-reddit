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

public class WordCountMapper
  extends Mapper<LongWritable, Text, Text, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    if (line.contains("trump") || line.contains("Trump") || line.contains("TRUMP")) {
    //if (line.contains("clinton") || line.contains("Clinton") || line.contains("CLINTON")) {
    String [] tokens = line.split(",");
    if (tokens.length > 0) {
    tokens[0] = tokens[0].replace(".","");
    tokens[0] = tokens[0].replace("!","");
    tokens[0] = tokens[0].replace("?","");
    tokens[0] = tokens[0].replace("\"", "");
    String [] comment = tokens[0].split("\\s");
    if (comment.length > 0) {
    for (String word: comment) {
        word = word.toLowerCase();
        if (word.length() > 1) {
        char c = word.charAt(0);
        if (Character.isLetter(c) || c == '#'){
        context.write(new Text(word), new IntWritable(1));
        }
}
}
}
    }
    }
  }
}

