package realworld.ch04;

import doit.ch04.WordCount;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

public class Facebook {
    public static class MyMappter extends Mapper<Text, Text, Text, Text> {
        private static final Log log = LogFactory.getLog(MyMappter.class);

        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String thisPerson = key.toString();
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ,");

            while(tokenizer.hasMoreTokens()) {
                String friend = tokenizer.nextToken();

                if(thisPerson.compareTo(friend) < 0) {
                    outKey.set(thisPerson + "," + friend);
                } else {
                    outKey.set(friend + "," + thisPerson);
                }
                log.info("MMMMMMMMM:" + outKey.toString() + ":" + value.toString());
                context.write(outKey, value);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private static final Log log = LogFactory.getLog(MyReducer.class);

        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] pair = key.toString().split(",");
            log.info("RRRRRRRRRRR6:" + pair[0] + "," + pair[1]);

            ArrayList<HashSet<String>> listOfFriendsSet = new ArrayList<>();
            for(Text value : values) {
                log.info("RRRRRRRRRRR1:" + key.toString() + ":" + value.toString());

                HashSet<String> friendsSet = new HashSet<>();

                StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ,");
                while(tokenizer.hasMoreTokens()) {
                    String friend = tokenizer.nextToken();
                    log.info("RRRRRRRRRRR7:" + friend);
                    if(!friend.equals(pair[0]) && !friend.equals(pair[1])) {
                        friendsSet.add(friend);
                    }
                }
                log.info("RRRRRRRRRRR5:" + friendsSet);

                if(friendsSet.size() > 0) {
                    listOfFriendsSet.add(friendsSet);
                }
            }
            log.info("RRRRRRRRRRR2:" + key.toString() + ":" + listOfFriendsSet.size());

            if(listOfFriendsSet.size() == 2) {
                listOfFriendsSet.get(0).retainAll(listOfFriendsSet.get(1));
                if(listOfFriendsSet.get(0).size() > 0) {
                    log.info("RRRRRRRRRRR3:" + key.toString() + ":" + listOfFriendsSet.get(0).size());

                    StringBuilder sb = new StringBuilder();
                    boolean first = true;

                    for (String friend : listOfFriendsSet.get(0)) {
                        if (first) {
                            first = false;
                        } else {
                            sb.append(",");
                        }
                        sb.append(friend);
                    }

                    outputValue.set(sb.toString());

                    log.info("RRRRRRRRRRR4:" + key.toString() + ":" + outputValue.toString());

                    context.write(key, outputValue);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Facebook");

        job.setJarByClass(Facebook.class);
        job.setMapperClass(Facebook.MyMappter.class);
        job.setReducerClass(Facebook.MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
