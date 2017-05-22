package util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SumCountTuple implements Writable {
    private float count = 0;
    private float sum;

    public SumCountTuple(int sum, int count){
        this.sum = sum;
        this.count = count;
    }

    public void readFields(DataInput in) throws IOException {
        count   = in.readFloat();
        sum     = in.readFloat();
    }

    public void write(DataOutput out) throws IOException {
        out.writeFloat(count);
        out.writeFloat(sum);
    }

    public float getCount() {
        return count;
    }

    public float getSum() {
        return sum;
    }

    public void setCount(float count) {
        this.count = count;
    }

    public void setSum(float sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return count + "\t" + sum;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Float.floatToIntBits(sum);
        result = prime * result + Float.floatToIntBits(count);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SumCountTuple other = (SumCountTuple) obj;
        if (Float.floatToIntBits(sum) != Float.floatToIntBits(other.sum))
            return false;
        if (Float.floatToIntBits(count) != Float.floatToIntBits(other.count))
            return false;
        return true;
    }

}