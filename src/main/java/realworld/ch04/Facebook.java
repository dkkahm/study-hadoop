package realworld.ch04;

import doit.ch04.WordCount;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.util.*;

public class Facebook {
    public static class MyMappter extends Mapper<Text, Text, FriendPair, FriendArray> {
        // private static final Log log = LogFactory.getLog(MyMappter.class);
        private Logger log = Logger.getLogger(MyMappter.class);

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String person = key.toString();
            String friends = value.toString();

            Friend f1 = populateFriend(person);
            List<Friend> friendList = populateFriendList(friends);
            Friend[] friendArray = Arrays.copyOf(friendList.toArray(), friendList.toArray().length, Friend[].class);
            FriendArray farray = new FriendArray(Friend.class, friendArray);

            for(Friend f2 : friendArray) {
                FriendPair fpair = new FriendPair(f1, f2);
                context.write(fpair, farray);
                log.info(fpair + "....." + farray);
            }
        }

        private Friend populateFriend(String friendJson) {
            JSONParser parser = new JSONParser();
            Friend friend = null;
            try {
                Object obj = (Object)parser.parse(friendJson);
                JSONObject jsonObject = (JSONObject) obj;

                Long lid = (long)jsonObject.get("id");
                IntWritable id = new IntWritable(lid.intValue());
                Text name = new Text((String)jsonObject.get("name"));
                Text hometown = new Text((String)jsonObject.get("hometown"));
                friend = new Friend(id, name, hometown);
            } catch (ParseException e) {
                e.printStackTrace();
            }

            return friend;
        }

        private List<Friend> populateFriendList(String friendsJson) {
            List<Friend> friendList = new ArrayList<>();

            try {
                JSONParser parser = new JSONParser();
                Object obj = (Object)parser.parse(friendsJson);
                JSONArray jsonarray = (JSONArray) obj;

                for(Object jobj : jsonarray) {
                    JSONObject entry = (JSONObject) jobj;
                    Long lid = (long) entry.get("id");
                    IntWritable id = new IntWritable(lid.intValue());
                    Text name = new Text((String) entry.get("name"));
                    Text hometown = new Text((String) entry.get("hometown"));
                    Friend friend = new Friend(id, name, hometown);
                    friendList.add(friend);
                }
            } catch(ParseException e) {
                e.printStackTrace();
            }

            return friendList;
        }
    }

    public static class MyReducer extends Reducer<FriendPair, FriendArray, FriendPair, FriendArray> {
        // private static final Log log = LogFactory.getLog(MyReducer.class);
        private Logger log = Logger.getLogger(MyReducer.class);

        @Override
        protected void reduce(FriendPair key, Iterable<FriendArray> values, Context context) throws IOException, InterruptedException {
            List<Friend[]> flist = new ArrayList<>();
            List<Friend> commonFriendList = new ArrayList<>();
            int count = 0;

            for(FriendArray farray : values) {
                Friend[] f = Arrays.copyOf(farray.get(), farray.get().length, Friend[].class);
                flist.add(f);
                count ++;
            }

            if(count != 2)
                return;

            for(Friend outerf : flist.get(0)) {
                for(Friend innerf : flist.get(1)) {
                    if(outerf.equals(innerf))
                        commonFriendList.add(innerf);
                }
            }

            Friend[] commonFriendsArray = Arrays.copyOf(commonFriendList.toArray(), commonFriendList.toArray().length, Friend[].class);
            context.write(key, new FriendArray(Friend.class, commonFriendsArray));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = new Job(conf, "Facebook");

        job.setJarByClass(Facebook.class);
        job.setMapperClass(Facebook.MyMappter.class);
        job.setReducerClass(Facebook.MyReducer.class);

        job.setOutputKeyClass(FriendPair.class);
        job.setOutputValueClass(FriendArray.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.waitForCompletion(true);
    }
}
