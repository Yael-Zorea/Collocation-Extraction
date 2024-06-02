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

public class StepCalcNpmiAndSum {
    // (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade# #N, value: N)
    //      (key: decade#w1w2#Cw1, value: Cw1 ) , (key: decade#w1w2#Cw2, value: Cw2 ) --> 
    //          (key: decade#w1w2#Npmi, value: Npmiw1w2) (key: decade# #NpmiSum, value: NpmiSum)
    
    // Calc Npmi = PMIw1w2 / (-log[Pw1w2])
    // PMIw1w2 = log(Cw1w2) + log(N) - log(Cw1) - log(Cw2) ; Pw1w2 = Cw1w2 / N 

    public static class MapperClass extends Mapper<LongWritable,Text,Text,TaggedCounter> {
    
        @Override
        // (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade# #N, value: N)
        //      (key: decade#w1w2#Cw1, value: Cw1 ) , (key: decade#w1w2#Cw2, value: Cw2 )
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // line format: decade TAB word1 TAB word2 TAB valueType TAB countValue
            String[] lineData = value.toString().split(Defns.TAB);
            Long decade = Long.parseLong(lineData[0]);
            String word1 = lineData[1];
            String word2 = lineData[2];
            Defns.ValueType valueType = Defns.ValueType.valueOf(lineData[3]);
            Long count = Long.parseLong(lineData[4]);
            
            if(valueType == Defns.ValueType.N){
                Text newKey = new Text(decade + Defns.TAB + Defns.first + Defns.TAB + Defns.first + Defns.TAB + valueType);
                TaggedCounter newCount = new TaggedCounter(count);
                context.write(newKey, newCount);
            }
            else{
                // For bringing the valuews of Cw1w2, Cw1, Cw2 together:
                Text newKey = new Text(decade + Defns.TAB + word1 + Defns.TAB + word2 + Defns.TAB + Defns.ValueType.Collab);
                TaggedCounter newCount = new TaggedCounter(count, valueType);
                context.write(newKey, newCount);
            }
            // The map only convert the text to our data structure, and send it to the reducers
        }
    
    }

    public static class PartitionerClass extends Partitioner<Text,TaggedCounter> {
        @Override
        // (key: decade# #N, values: {N#N})
        //          (key: decade#w1w2#Collab, values: {X} )  when X is some subset of {Cw1w2#Cw1w2, Cw1#Cw1, Cw2#Cw2} 
        public int getPartition(Text key, TaggedCounter value, int numPartitions) {
            String decade = key.toString().split(Defns.TAB)[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    public static class ReducerClass extends Reducer<Text,TaggedCounter,Text,DoubleWritable> {
        private long currentDecade;
        private double N;
        private double npmisSum;

        @Override
        public void setup(Context context)  throws IOException, InterruptedException {
            currentDecade = 0;
            N = 0;
            npmisSum = 0;
        }

        @Override
        // (key: decade# #N, values: {N#N})
        //          (key: decade#w1w2#Collab, values: {Cw1w2#Cw1w2, Cw1#Cw1, Cw2#Cw2} )
        public void reduce(Text key, Iterable<TaggedCounter> values, Context context) throws IOException,  InterruptedException {
            String[] lineData = key.toString().split(Defns.TAB);
            Long decade = Long.parseLong(lineData[0]);
            String word1 = lineData[1];
            String word2 = lineData[2];
            //Defns.ValueType valueType = Defns.ValueType.valueOf(lineData[3]);
            
            if(currentDecade != decade){ // Got to a new decade (key: decade# #N, value: {N})
                if(currentDecade != 0){ // Not the first time -> need to write the sum of the npmis of the previous decade:
                    Text sumKey = new Text(currentDecade + Defns.TAB + Defns.first + Defns.TAB + Defns.first + Defns.TAB + Defns.ValueType.SumNpmis);
                    context.write(sumKey, new DoubleWritable(npmisSum));
                    npmisSum = 0;
                }
                currentDecade = decade;
                for(TaggedCounter value : values){// There supposed to be only one value
                    N = (double)value.getCount();
                    //String keyS = decade + Defns.TAB + word1 + Defns.TAB + word2;
                    //context.write(new Text(keyS + Defns.TAB + Defns.ValueType.N), new DoubleWritable(N)); 
                }
            }
            else{ // (key: decade#w1w2#Collab, values: {Cw1w2#Cw1w2, Cw1#Cw1, Cw2#Cw2} )
                long Cw1w2 = 0, Cw1 = 0, Cw2 = 0;
                for (TaggedCounter value : values) {
                    if(value.getValueType() == Defns.ValueType.Cw1w2)
                        Cw1w2 = value.getCount();
                    else if(value.getValueType() == Defns.ValueType.Cw1)
                        Cw1 = value.getCount();
                    else if(value.getValueType() == Defns.ValueType.Cw2)
                        Cw2 = value.getCount();
                }
                
                Text newKey = new Text(decade + Defns.TAB + word1 + Defns.TAB + word2 + Defns.TAB + Defns.ValueType.Npmi);
                if((Cw1w2 !=0 && Cw1 != 0 && Cw2 != 0) && (Cw1w2*N > Cw1*Cw2)){ 
                    // PMIw1w2 = log(Cw1w2) + log(N) - log(Cw1) - log(Cw2) 
                    double PMIw1w2 = Math.log(Cw1w2) + Math.log(N) - (Math.log(Cw1) + Math.log(Cw2)); // = log(Cw1w2*N / (Cw1*Cw2))
                    // Pw1w2 = Cw1w2 / N 
                    double Pw1w2 = (double)(Cw1w2 / N);
                    // Npmi = PMIw1w2 / (-log[Pw1w2])
                    double Npmi = PMIw1w2 / (-Math.log(Pw1w2));
                    if(Npmi < 1){
                        npmisSum += Npmi;
                        context.write(newKey, new DoubleWritable(Npmi));
                    }
                }
                else{ // Test - not supposed to happen
                    if(Cw1w2 == 0)
                        context.write(newKey, new DoubleWritable(300)); // Test print to context
                    if(Cw1 == 0) 
                        context.write(newKey, new DoubleWritable(100)); // Test print to context
                    if(Cw2 == 0)
                        context.write(newKey, new DoubleWritable(200)); // Test print to context
                }

            }
                
        }
       
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Emit to the last decade the sum of the npmis
            if(currentDecade != 0){
                Text sumKey = new Text(currentDecade + Defns.TAB + Defns.first + Defns.TAB + Defns.first + Defns.TAB + Defns.ValueType.SumNpmis);
                context.write(sumKey, new DoubleWritable(npmisSum));
            }
        }
            

    }

    public static void main(String[] args) throws Exception {
        // the inputs from all previous 3 steps
        String input1 = args[0]; 
        String input2 = args[1];
        String input3 = args[2];
        String output = args[3];
        System.out.println("Doing step 3! Calculating Npmi and SumNpmi.");

        Configuration conf = new Configuration();
    
        Job job = Job.getInstance(conf, "Step3: Calc Npmi ans SumNpmi"); 
        
        job.setJarByClass(StepCalcNpmiAndSum.class);
        
        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TaggedCounter.class);
        
        job.setPartitionerClass(PartitionerClass.class);
        
        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(input1));
        FileInputFormat.addInputPath(job, new Path(input2));
        FileInputFormat.addInputPath(job, new Path(input3));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}

   
