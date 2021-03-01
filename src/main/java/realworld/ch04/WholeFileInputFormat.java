package realworld.ch04;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.File;
import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<Text, Text> {
    private static final Log log = LogFactory.getLog(WholeFileInputFormat.class);

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeFileRecordReader reader = new WholeFileRecordReader();
        reader.initialize(inputSplit, context);
        return reader;
    }

    public class WholeFileRecordReader extends RecordReader<Text, Text> {
        private FileSplit split;
        private Configuration conf;

        private Text currKey = null;
        private Text currValue = null;
        private boolean fileProcessed = false;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.split = (FileSplit) inputSplit;
            this.conf = taskAttemptContext.getConfiguration();
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if(fileProcessed) {
                return false;
            }

            int fileLength = (int)split.getLength();
            byte[] result = new byte[fileLength];

            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(split.getPath());
                IOUtils.readFully(in, result, 0, fileLength);
            } finally {
                IOUtils.closeStream(in);
            }
            this.fileProcessed = true;

            Path file = split.getPath();
            this.currKey = new Text(file.getName());
            this.currValue = new Text(result);

            return true;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return currKey;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return currValue;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
