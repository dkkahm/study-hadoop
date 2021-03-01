package realworld.ch04;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestFacebook {
    @Test
    public void testFriend() {
        Friend anna = new Friend(new IntWritable(100), new Text("anna"), new Text("anna home"));
        Friend beth = new Friend(new IntWritable(200), new Text("beth"), new Text("beth home"));

        FriendPair f1 = new FriendPair(anna, beth);
        FriendPair f2 = new FriendPair(beth, anna);

        System.out.println(f1);
        System.out.println(f1.hashCode());

        System.out.println(f2);
        System.out.println(f2.hashCode());

        System.out.println(f1.equals(f2));
        System.out.println(f1.compareTo(f2));

        System.out.println(f2.equals(f1));
        System.out.println(f2.compareTo(f1));
    }
}
