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

public class StepCalcCw1 {
    // (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade# #N, value: N) --> 
    //          (key: decade#w1 #Cw1, value: Cw1 ) , (key: decade#w1w2#Cw1, value: Cw1 ) 

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
                // For counting Cw1:
                Text key1 = new Text(decade + Defns.TAB + word1 + Defns.TAB + Defns.first + Defns.TAB + Defns.ValueType.Cw1); 
                context.write(key1, new LongWritable(count));
                // Duplicating the bigram key for w1:
                Text key2 = new Text(decade + Defns.TAB + word1 + Defns.TAB + word2 + Defns.TAB + Defns.ValueType.Cw1); 
                context.write(key2, zero);
            }
            // We are not suppose to get other count types here
        }

    }

    public static class CombinerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        // ( key: decade#w1 #Cw1,  values: {occ1, ...) ) OR ( key: decade#w1w2#Cw1 , values: {0} )
        // The combiner doen't change the value for w1w2 keys!
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long Cw1 = 0;
            for(LongWritable val : values) {
                Cw1 += val.get();
            }
            context.write(key, new LongWritable(Cw1));

        }
    }

    public static class PartitionerClass extends Partitioner<Text,LongWritable> {
        @Override
        // ( key: decade#w1 #Cw1,  value: occ ) OR ( key: decade#w1w2#Cw1 , value: 0 )
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            /*String[] lineData = key.toString().split(Defns.TAB);
            String decadeW1 = lineData[0] + Defns.TAB + lineData[1];*/
            return Math.abs(key.toString().split(Defns.TAB)[0].hashCode() % numPartitions);
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {
        private static String currentW1;
        private static long Cw1;

        @Override
        protected void setup(Context context) {
            currentW1 = null;
            Cw1 = 0;
        }

        @Override
        // for each decade and w1: first-( key: decade#w1 #Cw1,  values: {occ1,...} ) ; then the others-( key: decade#w1w2#Cw1 , values: {0} ).....
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            String[] lineData = key.toString().split(Defns.TAB);
            String word1 = lineData[1];
    
            if(currentW1 == null || !currentW1.equals(word1)){ // We got to a new w1 (or the first time) : ( key: decade#w1 #Cw1,  values: {occ1,...} )
            currentW1 = word1;
                Cw1 = 0;
                for(LongWritable val : values)
                    Cw1 += val.get();
            }
            else{ // ( key: decade#w1w2#Cw1 , values: {0} )
                context.write(key, new LongWritable(Cw1));
            }
            
        }
    
    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        System.out.println("Doing step 1! Calculating Cw1.");

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Step1: Calc Cw1"); 

        job.setJarByClass(StepCalcCw1.class);

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
