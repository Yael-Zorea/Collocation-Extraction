import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepCalcCw2 {
    
    // (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade# #N, value: N) --> 
    //          (key: decade#w2 #Cw2, value: Cw2 ) , (key: decade#w1w2#Cw2, value: Cw2 ) 

    public static class MapperClass extends Mapper<LongWritable,Text,Text,LongWritable> {
        private LongWritable zero;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            zero = new LongWritable(0);
        }

        @Override
        // (key: decade#w1w2, value: Cw1w2) , (key: decade, value: N)
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // line format: decade TAB word1 TAB word2 TAB valueType TAB countValue
            String[] lineData = value.toString().split(Defns.TAB);
            Long decade = Long.parseLong(lineData[0]);
            String word1 = lineData[1];
            String word2 = lineData[2];
            Defns.ValueType valueType = Defns.ValueType.valueOf(lineData[3]);
            Long count = Long.parseLong(lineData[4]);
            
            if(valueType == Defns.ValueType.N)
                return; // We don't write it again
            else if(valueType == Defns.ValueType.Cw1w2){
                // For counting Cw2:
                Text key1 = new Text(decade + Defns.TAB + word2 + Defns.TAB + Defns.first + Defns.TAB + Defns.ValueType.Cw2); 
                context.write(key1, new LongWritable(count));
                // Duplicating the bigram key for w2:
                Text key2 = new Text(decade + Defns.TAB + word2 + Defns.TAB + word1 + Defns.TAB + Defns.ValueType.Cw2); 
                context.write(key2, zero);
            }
            // We are not suppose to get other count types here
        }
    
    }

    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        // ( key: decade#w2 #Cw2,  values: {occ1, ...) ) OR ( key: decade#w2w1#Cw2 , values: {0} )
        // The combiner doen't change the value for w1w2 keys!
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long Cw2 = 0;
            for(LongWritable val : values) {
                Cw2 += val.get();
            }
            context.write(key, new LongWritable(Cw2));
        }
    }

    public static class PartitionerClass extends Partitioner<Text,LongWritable> {
        @Override
        // ( key: decade#w2 #Cw2,  value: occ ) OR ( key: decade#w2w1#Cw2 , value: 0 )
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            /*String[] lineData = key.toString().split(Defns.TAB);
            String decadeW2 = lineData[0] + Defns.TAB + lineData[1];*/
            return Math.abs(key.toString().split(Defns.TAB)[0].hashCode() % numPartitions);
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static String w2;
        private static long Cw2;

        @Override
        protected void setup(Context context) {
            w2 = null;
            Cw2 = 0;
        }

        @Override
        // for each decade and w2: first-( key: decade#w2 #Cw2,  values: {occ1,...} ) ; then the others-( key: decade#w2w1#Cw2 , values: {0} ).....
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            String[] lineData = key.toString().split(Defns.TAB);
            Long decade = Long.parseLong(lineData[0]);
            String word2 = lineData[1].equals(Defns.first)? null : lineData[1];
            String word1 = lineData[2].equals(Defns.first)? null : lineData[2];
            
            if(w2 == null || !w2.equals(word2)){ // We got to a new w2 (or the first time) : ( key: decade#w1null#Cw2,  values: {occ1,...} )
                w2 = word2; 
                Cw2 = 0;
                for(LongWritable val : values)
                    Cw2 += val.get();
            }
            else{ // ( key: decade#w1w2#Cw2 , values: {0} )
            // Reverse the key for w1 before w2:
            Text newKey = new Text(decade + Defns.TAB + word1 + Defns.TAB + word2 + Defns.TAB + Defns.ValueType.Cw2);
                context.write(newKey, new LongWritable(Cw2));
            }
        }

    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        System.out.println("Doing step 2! Calculating Cw2.");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step2: Calc Cw2"); 

        job.setJarByClass(StepCalcCw2.class);

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setCombinerClass(CombinerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
