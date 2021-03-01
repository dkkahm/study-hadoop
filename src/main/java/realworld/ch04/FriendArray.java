package realworld.ch04;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;

public class FriendArray extends ArrayWritable {

    public FriendArray() {
        super(Friend.class);
    }

    public FriendArray(Class<? extends Writable> valueClass) {
        super(valueClass);
    }

    public FriendArray(Class<? extends Writable> valueClass, Writable[] values) {
        super(valueClass, values);
    }

    @Override
    public String toString() {
        Friend[] friendArray = Arrays.copyOf(get(), get().length, Friend[].class);
        String print = "";

        for(Friend f : friendArray)
            print += f;

        return print;
    }

}
