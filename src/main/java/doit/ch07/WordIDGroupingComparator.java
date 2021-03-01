package doit.ch07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordIDGroupingComparator extends WritableComparator {
    protected WordIDGroupingComparator() {
        super(WordID.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {

        // split to get natural key
        WordID k1 = (WordID)w1;
        WordID k2 = (WordID)w2;

        return k1.getWord().compareTo(k2.getWord());
    }
}

