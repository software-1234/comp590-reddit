
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

public class DNSLookupMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String[] tokens = value.toString().split(",");
                String comment = tokens[0];
                String word = "";
//                if (comment.contains("immigration") || comment.contains("immigrant") || comment.contains("immigrants")
//                                || comment.contains("honduras") || comment.contains("migrant") || comment.contains("migrants")
//                                || comment.contains("mexican") || comment.contains("mexicans") || comment.contains("deport")
//                                || comment.contains("deportation") || comment.contains("ice") || comment.contains("illegal")
//                                || comment.contains("illegals") || comment.contains("alien") || comment.contains("aliens")
//                                || comment.contains("xenophobia") || comment.contains("xenophobe") || comment.contains("xenophobic")
//                                || comment.contains("refugee") || comment.contains("residency") || comment.contains("immigrate")
//                                || comment.contains("immigrated") || comment.contains("resident") || comment.contains("undocumented")
//                                || comment.contains("asylum") || comment.contains("displaced") || comment.contains("flee")
//                                || comment.contains("genocide") || comment.contains("border") || comment.contains("wall")
//                                || comment.contains("borders") || comment.contains("walls") || comment.contains("mexico")
//                                || comment.contains("drugs") || comment.contains("barrier") || comment.contains("barriers")
//                                || comment.contains("deporting") || comment.contains("deported") || comment.contains("citizenship")
//                                || comment.contains("citizen") || comment.contains("citizens") || comment.contains("detain")
//                                || comment.contains("detainment") || comment.contains("detained") || comment.contains("detaining")
//                                || comment.contains("detains") || comment.contains("deports") || comment.contains("ethnic")
//                                || comment.contains("foreign") || comment.contains("foreigner") || comment.contains("foreigners")
//                                || comment.contains("global") || comment.contains("ethnically") || comment.contains("ethnicity")
//                                || comment.contains("ethnicities") || comment.contains("diaspora") || comment.contains("diasporas")
//                                || comment.contains("assimilation")) {
//                        word = "immigration";
//                        context.write(new Text(word), new Text(tokens[1]));
//                }
//                if (comment.contains("gender") || comment.contains("woman") || comment.contains("women")
//                                || comment.contains("girl") || comment.contains("girls") || comment.contains("females")
//                                || comment.contains("female") || comment.contains("feminine") || comment.contains("feminist")
//                                || comment.contains("feminists") || comment.contains("feminazis") || comment.contains("feminazi")
//                                || comment.contains("pussy") || comment.contains("rape") || comment.contains("rapist")
//                                || comment.contains("rapists") || comment.contains("rapes") || comment.contains("raper")
//                                || comment.contains("sex") || comment.contains("assault") || comment.contains("pedophile")
//                                || comment.contains("bigot") || comment.contains("women's") || comment.contains("breast")
//                                || comment.contains("misogyny") || comment.contains("misogynist") || comment.contains("penis")
//                                || comment.contains("porn") || comment.contains("raped") || comment.contains("sexual")
//                                || comment.contains("sexually") || comment.contains("metoo") || comment.contains("molest")
//                                || comment.contains("molested") || comment.contains("abort") || comment.contains("abortion")
//                                || comment.contains("pro-life") || comment.contains("pro-choice") || comment.contains("sexuality")
//                                || comment.contains("LGBT") || comment.contains("LGBTQ") || comment.contains("LGBTQIA")
//                                || comment.contains("LGBTQIA+") || comment.contains("gay") || comment.contains("gays")
//                                || comment.contains("rights") || comment.contains("lesbian") || comment.contains("lesbians")
//                                || comment.contains("tran") || comment.contains("trans") || comment.contains("transvestite")
//                                || comment.contains("fag") || comment.contains("faggot") || comment.contains("faggots")
//                                || comment.contains("loveislove") || comment.contains("homo") || comment.contains("homosexual")
//                                || comment.contains("homos") || comment.contains("homosexuality") || comment.contains("homosexuals")
//                                || comment.contains("gender") || comment.contains("gap")) {
//                        word = "gender/sexuality";
//                        context.write(new Text(word), new Text(tokens[1]));
//                }
                if (comment.contains("climate") || comment.contains("ice") || comment.contains("change")
                                || comment.contains("eco") || comment.contains("polar") || comment.contains("caps")
                                || comment.contains("earth") || comment.contains("planet") || comment.contains("sea")
                                || comment.contains("water") || comment.contains("pollution") || comment.contains("ozone")
                                || comment.contains("plastic") || comment.contains("emissions") || comment.contains("carbon")
                                || comment.contains("fuel") || comment.contains("fuels") || comment.contains("warming")
                                || comment.contains("greenhouse") || comment.contains("sea-level") || comment.contains("energy")
                                || comment.contains("renewable") || comment.contains("solar") || comment.contains("methane")
                                || comment.contains("oceans") || comment.contains("farm") || comment.contains("farms")
                                || comment.contains("farming") || comment.contains("agriculture") || comment.contains("ecosystem")
                                || comment.contains("population") || comment.contains("overpopulation")
                                || comment.contains("environment") || comment.contains("environmentalists") || comment.contains("green")
                                || comment.contains("cooling") || comment.contains("kyoto") || comment.contains("paris")
                                || comment.contains("clean") || comment.contains("garbage") || comment.contains("landfill")
                                || comment.contains("trash") || comment.contains("waste") || comment.contains("wasted")
                                || comment.contains("wasting") || comment.contains("wastes") || comment.contains("industrial")
                                || comment.contains("electricity") || comment.contains("cleaning") || comment.contains("fire")) {
                        word = "climate";
                        context.write(new Text(word), new Text(tokens[1]));
                }
//                if (comment.contains("weapon") || comment.contains("weapons") || comment.contains("gun")
//                                || comment.contains("guns") || comment.contains("bomb") || comment.contains("bombs")
//                                || comment.contains("rifle") || comment.contains("rifles") || comment.contains("shotgun")
//                                || comment.contains("shotguns") || comment.contains("handgun") || comment.contains("handguns")
//                                || comment.contains("grenade") || comment.contains("grenades") || comment.contains("kill")
//                                || comment.contains("killed") || comment.contains("killing") || comment.contains("killer")
//                                || comment.contains("murder") || comment.contains("murdered") || comment.contains("murders")
//                                || comment.contains("murdering") || comment.contains("shooting") || comment.contains("shot")
//                                || comment.contains("safety") || comment.contains("columbine") || comment.contains("terror")
//                                || comment.contains("terrorist") || comment.contains("terrorism") || comment.contains("terrorists")
//                                || comment.contains("dangerous") || comment.contains("nationalist") || comment.contains("violence")
//                                || comment.contains("amendment") || comment.contains("violent") || comment.contains("punch")
//                                || comment.contains("anti-gun") || comment.contains("concealed") || comment.contains("crime")
//                                || comment.contains("criminal") || comment.contains("criminals") || comment.contains("felon")
//                                || comment.contains("self-defense") || comment.contains("firearm") || comment.contains("pistol")
//                                || comment.contains("lethal") || comment.contains("dead") || comment.contains("deadly")) {
//                        word = "guns";
//                        context.write(new Text(word), new Text(tokens[1]));
//                }
//                if (comment.contains("white") || comment.contains("black") || comment.contains("african-american")
//                                || comment.contains("kkk") || comment.contains("klan") || comment.contains("supremacist")
//                                || comment.contains("supremacists") || comment.contains("supremacy") || comment.contains("race")
//                                || comment.contains("racism") || comment.contains("racist") || comment.contains("races")
//                                || comment.contains("racial") || comment.contains("discrimination") || comment.contains("segregation")
//                                || comment.contains("segregate") || comment.contains("discriminate") || comment.contains("profiling")
//                                || comment.contains("slur") || comment.contains("slurs") || comment.contains("brutality")
//                                || comment.contains("civil") || comment.contains("rights") || comment.contains("nazi")
//                                || comment.contains("hitler") || comment.contains("colored") || comment.contains("africa")
//                                || comment.contains("african") || comment.contains("privilege") || comment.contains("diversity")
//                                || comment.contains("diverse") || comment.contains("equality") || comment.contains("equity")
//                                || comment.contains("minority") || comment.contains("minorities") || comment.contains("color")
//                                || comment.contains("inclusion") || comment.contains("exclusion") || comment.contains("intersectional")
//                                || comment.contains("intersectionality") || comment.contains("hood") || comment.contains("ghetto")
//                                || comment.contains("bias") || comment.contains("ethnic") || comment.contains("ethnicity")
//                                || comment.contains("ethnicities") || comment.contains("appropriation") || comment.contains("culture")
//                                || comment.contains("cultural") || comment.contains("multicultural") || comment.contains("blm")
//                                || comment.contains("unite")) {
//                        word = "race";
//                        context.write(new Text(word), new Text(tokens[1]));
//                } else if (comment.contains("tax") || comment.contains("taxes") || comment.contains("economy")
//                                || comment.contains("economy's") || comment.contains("economies") || comment.contains("globalization")
//                                || comment.contains("trade") || comment.contains("trades") || comment.contains("deal")
//                                || comment.contains("deals") || comment.contains("domestic") || comment.contains("finance")
//                                || comment.contains("global") || comment.contains("money") || comment.contains("fortune")
//                                || comment.contains("consumer") || comment.contains("goods") || comment.contains("crash")
//                                || comment.contains("deficit") || comment.contains("economics") || comment.contains("fiscal")
//                                || comment.contains("debt") || comment.contains("international") || comment.contains("currency")
//                                || comment.contains("inflation") || comment.contains("invest") || comment.contains("investment")
//                                || comment.contains("investments") || comment.contains("budget") || comment.contains("spending")
//                                || comment.contains("finance") || comment.contains("growth") || comment.contains("gap")
//                                || comment.contains("wage") || comment.contains("corporate") || comment.contains("mergers")
//                                || comment.contains("capital") || comment.contains("income") || comment.contains("gains")
//                                || comment.contains("nafta") || comment.contains("tpp") || comment.contains("free")
//                                || comment.contains("tariffs") || comment.contains("imports") || comment.contains("exports")
//                                || comment.contains("bitcoin") || comment.contains("sales") || comment.contains("luxury")) {
//                        word = "economy";
//                        context.write(new Text(word), new Text(tokens[1]));
//                }
//                if (comment.contains("china") || comment.contains("russia") || comment.contains("international")
//                                || comment.contains("global") || comment.contains("globalization") || comment.contains("chinese")
//                                || comment.contains("russian") || comment.contains("policy") || comment.contains("trade")
//                                || comment.contains("foreign") || comment.contains("defense") || comment.contains("security")
//                                || comment.contains("cyber") || comment.contains("cybersecurity") || comment.contains("emails")
//                                || comment.contains("war") || comment.contains("syria") || comment.contains("terrorist")
//                                || comment.contains("terrorists") || comment.contains("terrorism") || comment.contains("jinping")
//                                || comment.contains("theresa") || comment.contains("merkel") || comment.contains("putin")
//                                || comment.contains("vlad") || comment.contains("obama") || comment.contains("baghdad")
//                                || comment.contains("iran") || comment.contains("iraq") || comment.contains("honduras")
//                                || comment.contains("mexico") || comment.contains("nato") || comment.contains("un")
//                                || comment.contains("taiwan") || comment.contains("korea") || comment.contains("jong")
//                                || comment.contains("jongun") || comment.contains("jong-un") || comment.contains("kim")
//                                || comment.contains("france") || comment.contains("trudeau") || comment.contains("nuclear")
//                                || comment.contains("trade") || comment.contains("deal") || comment.contains("missile")
//                                || comment.contains("missiles") || comment.contains("space") || comment.contains("taliban")
//                                || comment.contains("isis") || comment.contains("pacific")) {
//                        word = "foreign_policy";
//                        context.write(new Text(word), new Text(tokens[1]));
//                }
//                if (comment.contains("religion") || comment.contains("religious") || comment.contains("religions")
//                                || comment.contains("islam") || comment.contains("rightwing") || comment.contains("radical")
//                                || comment.contains("fundamental") || comment.contains("fundamentalists") || comment.contains("muslim")
//                                || comment.contains("muslims") || comment.contains("islamic") || comment.contains("burqa")
//                                || comment.contains("quran") || comment.contains("jihad") || comment.contains("akbar")
//                                || comment.contains("terrorism") || comment.contains("terrorist") || comment.contains("terrorists")
//                                || comment.contains("islamaphobe") || comment.contains("islamaphobic")
//                                || comment.contains("islamaphobic") || comment.contains("ban") || comment.contains("christian")
//                                || comment.contains("christians") || comment.contains("christianity") || comment.contains("bible")
//                                || comment.contains("moral") || comment.contains("morality") || comment.contains("republican")
//                                || comment.contains("jesus")) {
//                        word = "religion";
//                        context.write(new Text(word), new Text(tokens[1]));
//                }

        }
}

