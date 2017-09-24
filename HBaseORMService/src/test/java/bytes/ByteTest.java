package bytes;

import com.immortalcockroach.hbaseorm.util.Bytes;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by immortalCockroach on 8/31/17.
 */
public class ByteTest {
    public static void main(String[] args) {

        /*List<byte[]> list = new ArrayList<>();
        list.add(new byte[]{1, 2, 3});
        list.add(new byte[]{2, 4, 5});

        byte[][] array = new byte[list.size()][];

        list.toArray(array);

        for (byte[] b : array) {
            for (byte bb : b) {
                System.out.print(bb + " ");
            }
            System.out.println();
        }*/

        byte[] B = new byte[]{0, 1, -1, 64};
        System.out.println(Bytes.toInt(B));
        /*for (byte b : B) {
            System.out.print(b + " ");
        }*/

        System.out.println(Bytes.compareTo(Bytes.toBytes(130880), Bytes.toBytes(130624)));
    }
}
