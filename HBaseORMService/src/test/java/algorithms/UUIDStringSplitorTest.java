package algorithms;

import java.math.BigInteger;

/**
 * Created by immortalCockroach on 8/29/17.
 */
public class UUIDStringSplitorTest {
    public static void main(String[] args) {
        byteRangeTest();
        char ch = 27;
        System.out.println(ch);

    }

    private static void byteRangeTest() {
        String firstRow = "1903";
        String lastRow = "1F03";
        BigInteger lastRowInt = new BigInteger(lastRow, 16);
        BigInteger firstRowInt = new BigInteger(firstRow, 16);
        BigInteger range = lastRowInt.subtract(firstRowInt).add(BigInteger.ONE);
        System.out.println(range.intValue());
    }
}
