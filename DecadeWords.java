import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class DecadeWords implements WritableComparable<DecadeWords> {

    private Long decade;
    private String word1;
    private String word2;
    private Defns.ValueType valueType;
    private Double npmiVal;

    public DecadeWords() {
        this.decade = null;
        this.word1 = null;
        this.word2 = null;
        this.valueType = null;
        this.npmiVal = null;
    }

    public DecadeWords(Long decade) {
        this.decade = decade;
        this.word1 = null;
        this.word2 = null;
        this.valueType = Defns.ValueType.N;
        this.npmiVal = null;
    }

    /*
     * If both words are null, the valueType is set to be N
     * If only wi is null, the valueType is set to be the Cwj (j!=i)
     * And if both words are non-null, the valueType is set to be Cw1w2 as default (not Pmi or P)
     */
    public DecadeWords(Long decade, String word1, String word2) {
        this.decade = decade;
        this.word1 = word1;
        this.word2 = word2;
        if( word1 == null && word2 == null)
            this.valueType = Defns.ValueType.N; 
        else if(word2 == null)
            this.valueType = Defns.ValueType.Cw1;
        else if(word1 == null)
            this.valueType = Defns.ValueType.Cw2;
        else 
            this.valueType = Defns.ValueType.Cw1w2; 
        this.npmiVal = null;
        
    }

    public DecadeWords(Long decade, String word1, String word2, Defns.ValueType valueType) { 
        // for duplicating a DecadeWords object
        if((valueType == Defns.ValueType.Cw1w2 || valueType == Defns.ValueType.Npmi || valueType == Defns.ValueType.Collab) && (word1 == null || word2 == null)) // NEW: removed || valueType == Defns.ValueType.Pmi_partial
            throw new IllegalArgumentException("[DEBUG]: Error creating DecadeWords obj: word1 and word2 must be non-null for valueType " + valueType.name() + " but got " + word1 + " and " + word2);
        if((valueType == Defns.ValueType.N || valueType == Defns.ValueType.SumNpmis) && (word1 != null || word2 != null))
            throw new IllegalArgumentException("[DEBUG]: Error creating DecadeWords obj: word1 and word2 must be null for valueType " + valueType.name() + " but got " + word1 + " and " + word2);
        this.decade = decade;
        this.word1 = word1;
        this.word2 = word2;
        this.valueType = valueType;
        this.npmiVal = null;
    }

    public DecadeWords(Long decade, String word1, String word2, Double npmi) { 
        this.decade = decade;
        if(word1 == null || word2 == null)
            throw new IllegalArgumentException("[DEBUG]: Error creating DecadeWords obj: word1 and word2 must be non-null for valueType NpmiVal but got " + word1 + " and " + word2);
        this.word1 = word1;
        this.word2 = word2;
        this.valueType = Defns.ValueType.NpmiWithVal;
        this.npmiVal = npmi;
    }

    public Long getDecade() { return this.decade; }
    public String getWord1() { return this.word1; }
    public String getWord2() { return this.word2; }
    public Defns.ValueType getValueType() { return this.valueType; }
    public Double getNpmiVal() { return this.npmiVal; } // NEW

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(decade);
        //dataOutput.writeUTF(word1);
        //dataOutput.writeUTF(word2);
        writeNullableString(dataOutput, word1);
        writeNullableString(dataOutput, word2);
        dataOutput.writeUTF(valueType.name());
        writeNullableDouble(dataOutput, npmiVal); // NEW
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        decade = dataInput.readLong();
        //word1 = dataInput.readUTF();
        //word2 = dataInput.readUTF();
        word1 = readNullableString(dataInput);
        word2 = readNullableString(dataInput);
        valueType = Defns.ValueType.valueOf(dataInput.readUTF());
        npmiVal = readNullableDouble(dataInput); // NEW
    }

    private void writeNullableString(DataOutput dataOutput, String value) throws IOException {
        if (value != null) {
            dataOutput.writeBoolean(true); // Signal that the value is not null
            dataOutput.writeUTF(value);
        } else {
            dataOutput.writeBoolean(false); // Signal that the value is null
        }
    }

    private String readNullableString(DataInput dataInput) throws IOException {
        if (dataInput.readBoolean()) {
            return dataInput.readUTF(); // Read the string if it's not null
        } else {
            return null; // Return null if the value is null
        }
    }

    private void writeNullableDouble(DataOutput dataOutput, Double value) throws IOException {
        if (value != null) {
            dataOutput.writeBoolean(true); // Signal that the value is not null
            dataOutput.writeDouble(value);
        } else {
            dataOutput.writeBoolean(false); // Signal that the value is null
        }
    }

    private Double readNullableDouble(DataInput dataInput) throws IOException {
        if (dataInput.readBoolean()) {
            return dataInput.readDouble(); // Read the string if it's not null
        } else {
            return null; // Return null if the value is null
        }
    }

    @Override
    public int compareTo(DecadeWords other) {
        int result = decade.compareTo(other.decade);
        if(result == 0){
            if( this.valueType == other.valueType){
                if(this.valueType == Defns.ValueType.Cw2)
                    result = lexicographicallyPairCompare(this.word2, this.word1, other.word2, other.word1); // Reverse lex compare
                else if(this.valueType == Defns.ValueType.NpmiWithVal) // NEW
                    result = other.npmiVal.compareTo(this.npmiVal); // Compare the npmi values for decreasing order
                else
                    result = lexicographicallyPairCompare(this.word1, this.word2, other.word1, other.word2); // Normal lex compare
            }
            else{
                /*result = this.valueType.compareTo(other.valueType);  
                // Compares this enum with the specified object for ORDER. Returns a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object. Enum constants are only comparable to other enum constants of the same enum type.
                // so we will get: {N < Cw1w2 < Cw1 < Cw2 < Pmi_N <Npmi} */

                // We want: { N < Cw1w2 = Cw1 = Cw2 = Collab < Npmi = sumNpmis < NpmiVal}
                // In steps 1,2,3 we just need N < Cw1w2, and in step 4 we need N < Cw1w2 = Cw1 = Cw2 < Pmi_N = P_N < Npmi
                // - we need Cw1w2 = Cw1 = Cw2 because we want to treat them as the SAME key in the reducer, and we will get (key:d#w1w2#x, values:Cw1w2,Cw1,Cw2) 
                // - we need  ... = Pmi_N < Npmi because if we can't remove the combiner, we need to calculate what we can without the N part (and remember thet Cw1w2, Cw1, Cw2 are treated as the same key, but they are not from the same files) 

                if(this.valueType == Defns.ValueType.N) // then other.valueType != N, so this is "smaller"
                    return -1;
                else if(other.valueType == Defns.ValueType.N) // then this.valueType != N, so this is "bigger"
                    return 1;
                else if(this.valueType == Defns.ValueType.Npmi || this.valueType == Defns.ValueType.SumNpmis) // NEW then other.value != Npmi & sumNpmis, so this is "bigger" iff other.valueType != NpmiVal
                    return (other.valueType == Defns.ValueType.NpmiWithVal) ? -1 : 1;
                else if(other.valueType == Defns.ValueType.Npmi || other.valueType == Defns.ValueType.SumNpmis) // NEW then this.value != Npmi & sumNpmis, so this is "bigger" iff this.valueType == NpmiVal
                    return (this.valueType == Defns.ValueType.NpmiWithVal) ? 1 : -1;
                else // both valueType are from {Cw1w2, Cw1, Cw2, Collab} --> lexico compare of the words
                    result = lexicographicallyPairCompare(this.word1, this.word2, other.word1, other.word2);
            }
        }
        return result;
    }

    private int lexicographicallyPairCompare(String str1, String str2, String str3, String str4) {
        int result = compare(str1, str3);
        if(result == 0)
            result = compare(str2, str4);
        return result;
    }

    /**
     * When we have <decade, N>, w1 and w2 will be null, and we want it to be first.
     * @return str1 - str2
     */
    private int compare(String str1, String str2) {
        if(str1 == null && str2 == null)
            return 0;
        else if(str1 == null)
            return -1;
        else if(str2 == null)
            return 1;
        else
            return str1.compareTo(str2);
    }

    public String toString() { 
        if(word1 == null || word2 == null)
            return decade + Defns.TAB + (word1 == null? "NULL" : word1) + Defns.TAB + (word2 == null? "NULL" : word2) + Defns.TAB + valueType.name();
        // both words are not null, and we need to print them in the order w1 w2 even if it's hebrew
        int maxLength = Math.max(word1.length(), word2.length());
        String formatString = "%" + maxLength + "s%s%" + maxLength + "s%s%" + maxLength + "s%s%" + maxLength + "s";
        String output = String.format(formatString, decade, Defns.TAB, word1, Defns.TAB, word2, Defns.TAB, valueType.name());
        return output;
        /*if(containsHebrew(word1) || containsHebrew(word2))
            return decade + Defns.TAB + (word2 == null? "NULL" : word2) + Defns.TAB + (word1 == null? "NULL" : word1) + Defns.TAB + valueType.name();
        else
            return decade + Defns.TAB + (word1 == null? "NULL" : word1) + Defns.TAB + (word2 == null? "NULL" : word2) + Defns.TAB + valueType.name();
        */
    }
    

}
