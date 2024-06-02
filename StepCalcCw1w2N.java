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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StepCalcCw1w2N {
    // (key: lineID, value: line) --> (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade# #N, value: N)
    
    public static class MapperClass extends Mapper<LongWritable,Text,Text,LongWritable> {
    
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            // line format: bigram TAB year TAB match_count TAB volume_count NEWLINE
            String[] lineData = value.toString().split(Defns.TAB);
            String[] bigram = lineData[0].split(" ");
            Long year = Long.parseLong(lineData[1]);
            Long matchCount = Long.parseLong(lineData[2]);
            
            if(bigram.length == 2) { // Apparently, there are some bigrams that don't have 2 words 🙄
                Long decade = year - (year % 10);
                String word1 = bigram[0];
                String word2 = bigram[1];

                if(!Defns.isStopWord(word1) && !Defns.isStopWord(word2)) {
                    Text key1 = new Text(decade + Defns.TAB + word1 + Defns.TAB + word2 + Defns.TAB + Defns.ValueType.Cw1w2);
                    LongWritable value1 = new LongWritable(matchCount);

                    Text key2 = new Text(decade + Defns.TAB + Defns.first + Defns.TAB  + Defns.first + Defns.TAB + Defns.ValueType.N);

                    context.write(key1, value1);
                    context.write(key2, value1);
                }
            }
        }
    
    }

    public static class PartitionerClass extends Partitioner<Text,LongWritable> {
        @Override
        // ( key: decade# #N , value: occ ) OR ( key: decade#w1w2#Cw1w2,  value: occ )
        public int getPartition(Text key, LongWritable value, int numPartitions) {
            String decade = key.toString().split(Defns.TAB)[0];
            return Math.abs(decade.hashCode() % numPartitions);
        }
    }

    public static class ReducerClass extends Reducer<Text,LongWritable,Text,LongWritable> {

        @Override
        // ( key: decade# #N , values: {occ1, ...} ) OR ( key: decade#w1w2#Cw1w2,  values: {occ1, ...})
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            Long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }

    }

    public static void main(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        System.out.println("Doing step 0! Calculating Cw1w2 and N.");

        Configuration conf = new Configuration();
        //conf.set("mapred.max.split.size", "33554432"); // 32MB in bytes

        Job job = Job.getInstance(conf, "Step0: Calc Cw1w2 and N"); 

        job.setJarByClass(StepCalcCw1w2N.class);
        job.setInputFormatClass(SequenceFileInputFormat.class); 

        job.setMapperClass(MapperClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setCombinerClass(ReducerClass.class);
        job.setPartitionerClass(PartitionerClass.class);

        job.setReducerClass(ReducerClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
