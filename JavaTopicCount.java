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

public final class JavaTopicCount {

  // The first argument to the main function is the HDFS input file name
  // (specified as a parameter to the spark-submit command).  The
  // second argument is the HDFS output directory name (must not exist
  // when the program is run).

public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: JavaTopicCount <input file> <output file>");
      System.exit(1);
    }

    // Create session context for executing the job
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaTopicCount")
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
    // The RDD 'words' will have an entry for each instance of an 
    // IP address that appears in the input HDFS file (most addresses 
    // will appear more than once and be repeated in 'words').
    
    JavaRDD<String> words = lines.flatMap(
      new FlatMapFunction<String, String>() {
        @Override
        public Iterator<String> call(String s) {
              String[] tokens = s.split("\t");
                String word = tokens[0];
                String [] keyword = {"Null 0"};
                  if (word.equals("immigration") || word.equals("immigrant") || 
                      word.equals("immigrants") || word.equals("honduras") ||
                      word.equals("migrant") || word.equals("migrants") ||
                      word.equals("mexican") || word.equals("mexicans") ||
                      word.equals("deport") || word.equals("deportation") ||
                      word.equals("ice") || word.equals("illegal") ||
                      word.equals("illegals") || word.equals("alien") ||
                      word.equals("aliens") || word.equals("xenophobia") ||
                      word.equals("xenophobe") || word.equals("xenophobic") ||
                      word.equals("refugee") || word.equals("residency") ||
                      word.equals("immigrate") || word.equals("immigrated") ||
                      word.equals("resident") || word.equals("undocumented") ||
                      word.equals("asylum") || word.equals("displaced") ||
                      word.equals("flee") || word.equals("genocide") ||
                      word.equals("border") || word.equals("wall") ||
                      word.equals("borders") || word.equals("walls") ||
                      word.equals("mexico") || word.equals("drugs") ||
                      word.equals("barrier") || word.equals("barriers") ||
                      word.equals("deporting") || word.equals("deported") ||
                      word.equals("citizenship") || word.equals("citizen") ||
                      word.equals("citizens") || word.equals("detain") ||
                      word.equals("detainment") || word.equals("detained") ||
                      word.equals("detaining") || word.equals("detains") ||
                      word.equals("deports") || word.equals("ethnic") ||
                      word.equals("foreign") || word.equals("foreigner") ||
                      word.equals("foreigners") || word.equals("global") ||
                      word.equals("ethnically") || word.equals("ethnicity") ||
                      word.equals("ethnicities") || word.equals("diaspora") ||
                      word.equals("diasporas") || word.equals("assimilation")) {
                          word = "immigration" + " " + tokens[1];
                          keyword[0] = word;
                      } else if (word.equals("gender") || word.equals("woman") ||
                      word.equals("women") || word.equals("girl") || 
                      word.equals("girls") || word.equals("females") ||
                      word.equals("female") || word.equals("feminine") ||
                      word.equals("feminist") || word.equals("feminists") ||
                      word.equals("feminazis") || word.equals("feminazi") ||
                      word.equals("pussy") || word.equals("rape") ||
                      word.equals("rapist") || word.equals("rapists") ||
                      word.equals("rapes") || word.equals("raper") ||
                      word.equals("sex") || word.equals("assault") ||
                      word.equals("pedophile") || word.equals("bigot") ||
                      word.equals("women's") || word.equals("breast") ||
                      word.equals("misogyny") || word.equals("misogynist") ||
                      word.equals("penis") || word.equals("porn") ||
                      word.equals("raped") || word.equals("sexual") ||
                      word.equals("sexually") || word.equals("metoo") ||
                      word.equals("molest") || word.equals("molested") ||
                      word.equals("abort") || word.equals("abortion") ||
                      word.equals("pro-life") || word.equals("pro-choice") ||
                      word.equals("sexuality") || word.equals("LGBT") ||
                      word.equals("LGBTQ") || word.equals("LGBTQIA") ||
                      word.equals("LGBTQIA+") || word.equals("gay") ||
                      word.equals("gays") || word.equals("rights") ||
                      word.equals("lesbian") || word.equals("lesbians") ||
                      word.equals("tran") || word.equals("trans") ||
                      word.equals("transvestite") || word.equals("fag") ||
                      word.equals("faggot") || word.equals("faggots") ||
                      word.equals("loveislove") || word.equals("homo") ||
                      word.equals("homosexual") || word.equals("homos") ||
                      word.equals("homosexuality") || word.equals("homosexuals") ||
                      word.equals("gender") || word.equals("gap")) {
		         word = "gender/sexuality" + " " + tokens[1];
                          keyword[0] = word;
                      } else if (word.equals("climate") || word.equals("ice") ||
                      word.equals("change") || word.equals("eco") || 
                      word.equals("polar") || word.equals("caps") ||
                      word.equals("earth") || word.equals("planet") ||
                      word.equals("sea") || word.equals("water") ||
                      word.equals("pollution") || word.equals("ozone") ||
                      word.equals("plastic") || word.equals("emissions") ||
                      word.equals("carbon") || word.equals("fuel") ||
                      word.equals("fuels") || word.equals("warming") ||
                      word.equals("greenhouse") || word.equals("sea-level") ||
                      word.equals("energy") || word.equals("renewable") ||
                      word.equals("solar") || word.equals("methane") ||
                      word.equals("oceans") || word.equals("farm") ||
                      word.equals("farms") || word.equals("farming") ||
                      word.equals("agriculture") || word.equals("ecosystem") ||
                      word.equals("population") || word.equals("overpopulation") ||
                      word.equals("environment") || word.equals("environmentalists") ||
                      word.equals("green") || word.equals("cooling") ||
                      word.equals("kyoto") || word.equals("paris") ||
                      word.equals("clean") || word.equals("garbage") ||
                      word.equals("landfill") || word.equals("trash") ||
                      word.equals("waste") || word.equals("wasted") ||
                      word.equals("wasting") || word.equals("wastes") ||
                      word.equals("industrial") || word.equals("electricity") ||
                      word.equals("cleaning") || word.equals("fire")) {
                          word = "climate" + " " + tokens[1];
                          keyword[0] = word;
                      } else if (word.equals("weapon") || word.equals("weapons") ||
                                 word.equals("gun") || word.equals("guns") ||
                                 word.equals("bomb") || word.equals("bombs") ||
                                 word.equals("rifle") || word.equals("rifles") ||
                                 word.equals("shotgun") || word.equals("shotguns") ||
                                 word.equals("handgun") || word.equals("handguns") ||
                                 word.equals("grenade") || word.equals("grenades") ||
                                 word.equals("kill") || word.equals("killed") ||
                                 word.equals("killing") || word.equals("killer") ||
                                 word.equals("murder") || word.equals("murdered") ||
                                 word.equals("murders") || word.equals("murdering") ||
                                 word.equals("shooting") || word.equals("shot") ||
                                 word.equals("safety") || word.equals("columbine") ||
                                 word.equals("terror") || word.equals("terrorist") ||
                                 word.equals("terrorism") || word.equals("terrorists") ||
                                 word.equals("dangerous") || word.equals("nationalist") ||
                                 word.equals("violence") || word.equals("amendment") ||
                                 word.equals("violent") || word.equals("punch") ||
                                 word.equals("anti-gun") || word.equals("concealed") ||
                                 word.equals("crime") || word.equals("criminal") ||
                                 word.equals("criminals") || word.equals("felon") ||
                                 word.equals("self-defense") || word.equals("firearm") ||
                                 word.equals("pistol") || word.equals("lethal") ||
                                 word.equals("dead") || word.equals("deadly")) {
				     word = "guns" + " " + tokens[1];
                                     keyword[0] = word;
                                 } else if (word.equals("white") || word.equals("black") ||
                                 word.equals("african-american") || word.equals("kkk") ||
                                 word.equals("klan") || word.equals("supremacist") ||
                                 word.equals("supremacists") || word.equals("supremacy") ||
                                 word.equals("race") || word.equals("racism") ||
                                 word.equals("racist") || word.equals("races") ||
                                 word.equals("racial") || word.equals("discrimination") ||
                                 word.equals("segregation") || word.equals("segregate") ||
                                 word.equals("discriminate") || word.equals("profiling") ||
                                 word.equals("slur") || word.equals("slurs") ||
                                 word.equals("brutality") || word.equals("civil") ||
                                 word.equals("rights") || word.equals("nazi") ||
                                 word.equals("hitler") || word.equals("colored") ||
                                 word.equals("africa") || word.equals("african") ||
                                 word.equals("privilege") || word.equals("diversity") ||
                                 word.equals("diverse") || word.equals("equality") ||
                                 word.equals("equity") || word.equals("minority") ||
                                 word.equals("minorities") || word.equals("color") ||
                                 word.equals("inclusion") || word.equals("exclusion") ||
                                 word.equals("intersectional") || word.equals("intersectionality") ||
                                 word.equals("hood") || word.equals("ghetto") ||
                                 word.equals("bias") || word.equals("ethnic") ||
                                 word.equals("ethnicity") || word.equals("ethnicities") ||
                                 word.equals("appropriation") || word.equals("culture") ||
                                 word.equals("cultural") || word.equals("multicultural") ||
                                 word.equals("blm") || word.equals("unite")) {
                                 word = "race" + " " + tokens[1];
                                     keyword[0] = word;
				} else if (word.equals("tax") || word.equals("taxes") ||
                                word.equals("economy")  || word.equals("economy's") || 
                                word.equals("economies") || word.equals("globalization") ||
                                word.equals("trade") || word.equals("trades") ||
                                word.equals("deal") || word.equals("deals") ||
                                word.equals("domestic") || word.equals("finance") ||
                                word.equals("global") || word.equals("money") ||
                                word.equals("fortune") || word.equals("consumer") ||
                                word.equals("goods") || word.equals("crash") ||
                                word.equals("deficit") || word.equals("economics") ||
                                word.equals("fiscal") || word.equals("debt") ||
                                word.equals("international") || word.equals("currency") ||
                                word.equals("inflation") || word.equals("invest") ||
                                word.equals("investment") || word.equals("investments") ||
                                word.equals("budget") || word.equals("spending") ||
                                word.equals("finance") || word.equals("growth") ||
                                word.equals("gap") || word.equals("wage") ||
                                word.equals("corporate") || word.equals("mergers") ||
                                word.equals("capital") || word.equals("income") ||
                                word.equals("gains") || word.equals("nafta") ||
                                word.equals("tpp") || word.equals("free") ||
                                word.equals("tariffs") || word.equals("imports") ||
                                word.equals("exports") || word.equals("bitcoin") ||
                                word.equals("sales") || word.equals("luxury")) {
                                   word = "economy" + " " + tokens[1];
                                     keyword[0] = word;
                                } else if (word.equals("china") || word.equals("russia") ||
                                word.equals("international") || word.equals("global") ||
                                word.equals("globalization") || word.equals("chinese") ||
                                word.equals("russian") || word.equals("policy") ||
                                word.equals("trade") || word.equals("foreign") ||
                                word.equals("defense") || word.equals("security") ||
                                word.equals("cyber") || word.equals("cybersecurity") ||
                                word.equals("emails") || word.equals("war") ||
                                word.equals("syria") || word.equals("terrorist") ||
                                word.equals("terrorists") || word.equals("terrorism") ||
                                word.equals("jinping") || word.equals("theresa") ||
                                word.equals("merkel") || word.equals("putin") || 
                                word.equals("vlad") || word.equals("obama") ||
                                word.equals("baghdad") || word.equals("iran") ||
                                word.equals("iraq") || word.equals("honduras") ||
                                word.equals("mexico") || word.equals("nato") ||
                                word.equals("un") || word.equals("taiwan") ||
                                word.equals("korea") || word.equals("jong") ||
                                word.equals("jongun") || word.equals("jong-un") ||
                                word.equals("kim") || word.equals("france") ||
                                word.equals("trudeau") || word.equals("nuclear") ||
                                word.equals("trade") || word.equals("deal") || 
                                word.equals("missile") || word.equals("missiles") ||
                                word.equals("space") || word.equals("taliban") ||
                                word.equals("isis") || word.equals("pacific")) {
                                   word = "foreign_policy" + " " + tokens[1];
                                     keyword[0] = word;
                                } else if (word.equals("religion") || word.equals("religious") ||
                                word.equals("religions") || word.equals("islam") ||
                                word.equals("rightwing") || word.equals("radical") ||
                                word.equals("fundamental") || word.equals("fundamentalists") ||
                                word.equals("muslim") || word.equals("muslims") ||
                                word.equals("islamic") || word.equals("burqa") ||
                                word.equals("quran") || word.equals("jihad") ||
                                word.equals("akbar") || word.equals("terrorism") ||
                                word.equals("terrorist") || word.equals("terrorists") ||
                                word.equals("islamaphobe") || word.equals("islamaphobic") ||
                                word.equals("islamaphobic") || word.equals("ban") ||
                                word.equals("christian") || word.equals("christians") ||
                                word.equals("christianity") || word.equals("bible") ||
                                word.equals("moral") || word.equals("morality") ||
                                word.equals("republican") || word.equals("jesus")) {
                                     word = "religion" + " " + tokens[1];
                                     keyword[0] = word;
                                }
          return Arrays.asList(keyword).iterator();
          
       }
     });

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
            String [] tokens = s.split(" ");
            return new Tuple2<>(tokens[0], Integer.parseInt(tokens[1]));
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

