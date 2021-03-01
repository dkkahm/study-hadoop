package doit.ch07;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordIDPartitioner extends Partitioner<WordID, Text> {
    protected WordIDPartitioner() {
    }

    @Override
    public int getPartition(WordID key, Text val, int numPartitions) {
        return (key.getWord().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
