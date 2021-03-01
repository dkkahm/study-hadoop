package realworld.ch04;

import org.apache.hadoop.io.WritableComparable;
import sun.reflect.generics.repository.FieldRepository;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendPair implements WritableComparable {
    Friend first;
    Friend second;

    public FriendPair() {
        this.first = new Friend();
        this.second = new Friend();
    }

    public FriendPair(Friend first, Friend second) {
        this.first = first;
        this.second = second;
    }

    public Friend getFirst() {
        return first;
    }

    public void setFirst(Friend first) {
        this.first = first;
    }

    public Friend getSecond() {
        return second;
    }

    public void setSecond(Friend second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    @Override
    public int compareTo(Object o) {
        FriendPair pair2 = (FriendPair) o;

        Friend this_first = getFirst();
        Friend this_second = getSecond();
        if(this_first.compareTo(this_second) > 0) {
            Friend t = this_first;
            this_first = this_second;
            this_second = t;
        }

        Friend that_first = pair2.getFirst();
        Friend that_second = pair2.getSecond();
        if(that_first.compareTo(that_second) > 0) {
            Friend t = that_first;
            that_first = that_second;
            that_second = t;
        }

        int cmp = this_first.compareTo(that_first);
        if(cmp != 0) return cmp;

        cmp = this_second.compareTo(that_second);
        if(cmp != 0) return cmp;

        return 0;
    }

    @Override
    public boolean equals(Object o) {
        FriendPair pair2 = (FriendPair) o;
        boolean eq = getFirst().equals(pair2.getFirst()) || getFirst().equals(pair2.getSecond());
        if(!eq) return eq;

        return getSecond().equals(pair2.getSecond()) || getSecond().equals(pair2.getFirst());
    }

    @Override
    public String toString() {
        return "[" + first + ";" + second + "]";
    }

    @Override
    public int hashCode() {
        return first.getId().hashCode() + second.getId().hashCode();
    }
}
