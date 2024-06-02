import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepFilterSortNpmi {
    // (key: decade#w1w2#Npmi, value: Npmiw1w2) (key: decade# #NpmiSum, value: NpmiSum) --> 
    //      (key: decade#w1w2#Npmi, value: Npmiw1w2) filtered by [(Npmi / SumNpmis) >= relMinNpmi] or [Npmi >= minNpmi], and sorted by Npmi in descending

    public static class MapperClass extends Mapper<LongWritable,Text,Text,DoubleWritable> {
    
        @Override
        // (key: decade#w1w2#Npmi, value: Npmiw1w2) , (key: decade# #SumNpmis, value: SumNpmis)
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // line format: decade TAB word1 TAB word2 TAB valueType TAB countValue
            String[] lineData = value.toString().split(Defns.TAB);
            Long decade = Long.parseLong(lineData[0]);
            String word1 = lineData[1];
            String word2 = lineData[2];
            Defns.ValueType valueType = Defns.ValueType.valueOf(lineData[3]); // All are supposed to be with valueTag Npmi or SumNpmis
            Double count = Double.parseDouble(lineData[4]);
            // We still can't filter here because we don't have the SumNpmis yet (and ones that will be deleted by minNpmi might be approved by relMinNpmi)
            
            if(valueType == Defns.ValueType.Npmi){
                double reverse = 1.0 - count;
                Text keyWithCount = new Text(decade + Defns.TAB + reverse + Defns.TAB + word1 + Defns.TAB + word2 + Defns.TAB + Defns.ValueType.NpmiWithVal); 
                context.write(keyWithCount, new DoubleWritable(count));
            }
            else if(valueType == Defns.ValueType.SumNpmis){
                Text sumKey = new Text(decade + Defns.TAB + Defns.first + Defns.TAB + Defns.first + Defns.TAB + Defns.first + Defns.TAB + Defns.ValueType.SumNpmis);
                context.write(sumKey, new DoubleWritable(count));
            }
        }
    
    }

    public static class PartitionerClass extends Partitioner<Text,DoubleWritable> {
        @Override
        // (key: decade#w1w2#SumNpmis, value: SumNpmis) ,(key: decade# #npmi#NpmiWithVal, value: Npmiw1w2) 
        public int getPartition(Text key, DoubleWritable value, int numPartitions) {
            String decade = key.toString().split(Defns.TAB)[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    public static class ReducerClass extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private long currentDecade;
        private double sumNpmis;
        double minNpmi;
        double relMinNpmi;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            currentDecade = 0;
            sumNpmis = 0;

            minNpmi = Double.parseDouble(context.getConfiguration().get("minNpmi","1"));
            relMinNpmi = Double.parseDouble(context.getConfiguration().get("relMinNpmi","1"));
            
            System.out.println("minNpmi: " + minNpmi + ", relMinNpmi: " + relMinNpmi);
        }

        @Override
        // For each decade: First-(key: decade# #SumNpmis, values: {SumNpmis}) , then sorted by their values-(key: decade#w1w2#npmi#NpmiWithVal, values: {Npmiw1w2}) 
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException,  InterruptedException {
            String[] lineData = key.toString().split(Defns.TAB);
            if(lineData.length < 4){ context.write(key,new DoubleWritable(500000000)); return;} // TODO
            Long decade = Long.parseLong(lineData[0]);
            String word1 = lineData[2];
            String word2 = lineData[3];
            //Defns.ValueType valueType = Defns.ValueType.valueOf(lineData[4]); 
            String cleanKey = decade + Defns.TAB + word1 + Defns.TAB + word2 + Defns.TAB;

            if(currentDecade != decade){ // Got to a new decade (key: decade#w1w2#SumNpmis, value: SumNpmis)
                currentDecade = decade;
                for(DoubleWritable value : values){ // There supposed to be only one value
                    sumNpmis = value.get();
                }
            }
            else{ // (key: decade#w1w2#npmi#NpmiWithVal, value: Npmiw1w2)
                // Filter by [(Npmi / SumNpmis) >= relMinNpmi] or [Npmi >= minNpmi]
                for(DoubleWritable value : values){ // There supposed to be only one value
                    double npmi = value.get();
                    if(((npmi / sumNpmis) >= relMinNpmi) || (npmi >= minNpmi)){ // Filter
                        context.write(new Text(cleanKey + Defns.ValueType.Npmi), value); 
                    }
                }
            }
                
        }
            

    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        System.out.println("Doing step 4! Filter and Sort Npmi.");

        Configuration conf = new Configuration();
        conf.set("minNpmi", args[2]);
        conf.set("relMinNpmi", args[3]);

        Job job = Job.getInstance(conf, "Step4: Filter & Sort Npmi");
        job.setJarByClass(StepFilterSortNpmi.class);

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(PartitionerClass.class);

        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
