package doit.ch07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

public class InversedIndex2 {
    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private Text word = new Text();
        HashSet<String> words = new HashSet<>();

        @Override
        protected void map(Text docId, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line, "\t\r\n\f |,!#\"$.'%&=+-_^@`~:?<>(){}[];*/");

            words.clear();
            while (tokenizer.hasMoreTokens()) {
                words.add(tokenizer.nextToken().toLowerCase());
            }

            Iterator it = words.iterator();
            while(it.hasNext()) {
                String v = (String)it.next();
                word.set(v);
                context.write(word, docId);
            }
        }
    }

    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                StringBuffer toReturn = new StringBuffer();
                boolean first = true;

                for (Text val : values) {
                    if (!first) {
                        toReturn.append(",");
                    } else {
                        first = false;
                    }

                    toReturn.append(val.toString());
                }
                context.write(key, new Text(toReturn.toString()));
            } catch (Exception e) {
                context.getCounter("Error", "Reducer Exception:" + key.toString()).increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Inverted Index");

        // if mapper outputs are different, call setMapOutputKeyClass and setMapOutputValueClass
        job.setJarByClass(InversedIndex2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(10);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
