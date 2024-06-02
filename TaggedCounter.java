import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TaggedCounter implements Writable{
    private Long count;
    private Defns.ValueType valueType;

    public TaggedCounter() {
        this.count = null;
        this.valueType = null;
    }

    public TaggedCounter(Long count) {
        this.count = count;
        this.valueType = Defns.ValueType.N;
    }

    public TaggedCounter(Long count, Defns.ValueType valueType) {
        this.count = count;
        this.valueType = valueType;
    }

    public Long getCount() { return this.count; }
    public Defns.ValueType getValueType() { return this.valueType; }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(count);
        dataOutput.writeUTF(valueType.name());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readLong();
        valueType = Defns.ValueType.valueOf(dataInput.readUTF());
    }
    
}