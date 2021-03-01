package doit.ch07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordID implements WritableComparable<WordID> {

    private String word;
    private Long docID;

    /**
     * Constructor.
     */
    public WordID() { }

    /**
     * Constructor.
     */
    public WordID(String word, long docID) {
        this.word = word;
        this.docID = docID;
    }

    @Override
    public String toString() {
        return (new StringBuilder())
                .append('{')
                .append(word)
                .append(',')
                .append(docID)
                .append('}')
                .toString();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = WritableUtils.readString(in);
        docID = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, word);
        out.writeLong(docID);
    }

    @Override
    public int compareTo(WordID o) {
        int result = word.compareTo(o.word);
        if(0 == result) {
            result = (int)(docID-(o.docID));
        }
        return result;
    }

    /**
     * Gets the Word.
     * @return word.
     */
    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    /**
     * Gets the docID.
     * @return docID.
     */
    public Long getDocID() {
        return docID;
    }

    public void setDocID(Long docID) {
        this.docID = docID;
    }
}