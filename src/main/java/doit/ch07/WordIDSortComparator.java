package doit.ch07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordIDSortComparator extends WritableComparator {
    protected WordIDSortComparator() {
        super(WordID.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {

        // split to get natural key
        WordID k1 = (WordID)w1;
        WordID k2 = (WordID)w2;

        int result = k1.compareTo(k2);
        return result;
    }
}
