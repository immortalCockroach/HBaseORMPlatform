package service.hbasemanager.entity.comparators;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.protobuf.generated.ComparatorProtos;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * int比较器
 * Created by immortalCockroach on 10/23/17.
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class IntComparator extends ByteArrayComparable {
    private Integer intValue;

    public IntComparator(int value) {
        super(Bytes.toBytes(value));
        this.intValue = value;
    }

    /**
     * @param pbBytes A pb serialized {@link BinaryComparator} instance
     * @return An instance of {@link BinaryComparator} made from <code>bytes</code>
     * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
     * @see #toByteArray
     */
    public static org.apache.hadoop.hbase.filter.LongComparator parseFrom(final byte[] pbBytes)
            throws DeserializationException {
        ComparatorProtos.LongComparator proto;
        try {
            proto = ComparatorProtos.LongComparator.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
        return new org.apache.hadoop.hbase.filter.LongComparator(Bytes.toLong(proto.getComparable().getValue().toByteArray()));
    }

    @Override
    public int compareTo(byte[] value, int offset, int length) {
        Integer that = Bytes.toInt(value, offset, length);
        return this.intValue.compareTo(that);
    }

    /**
     * @return The comparator serialized using pb
     */
    @Override
    public byte[] toByteArray() {
        ComparatorProtos.LongComparator.Builder builder =
                ComparatorProtos.LongComparator.newBuilder();
        builder.setComparable(this.convert());
        return builder.build().toByteArray();
    }

    public ComparatorProtos.ByteArrayComparable convert() {
        ComparatorProtos.ByteArrayComparable.Builder builder =
                ComparatorProtos.ByteArrayComparable.newBuilder();
        if (super.getValue() != null) builder.setValue(ByteStringer.wrap(super.getValue()));
        return builder.build();
    }

    /**
     * @param other
     * @return true if and only if the fields of the comparator that are serialized
     * are equal to the corresponding fields in other.  Used for testing.
     */
    boolean areSerializedFieldsEqual(IntComparator other) {
        if (other == this) return true;
        if (!(other instanceof IntComparator)) return false;

        return this.areSerializedFieldsEqual(other);
    }

    boolean areSerializedFieldsEqual(ByteArrayComparable other) {
        if (other == this) {
            return true;
        }

        return Bytes.equals(this.getValue(), other.getValue());
    }
}
