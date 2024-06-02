import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class CollocationExtraction { 
    
    public static void main(String[] args) {
        if (args == null || args.length != 2) {
            System.out.println("ERROR! Invalid input usage: java CollocationExtraction <minPmi> <relMinPmi>");
            System.exit(1);
        }

        Defns.minNpmi = args[0];
        Defns.relMinNpmi = args[1];
        
        /*Defns.createBucketIfNotExists(Defns.PROJECT_NAME);
        Defns.uploadFileToS3(Defns.PROJECT_NAME, "smallInput1.txt", "C:/Users/יעל/מבוזרות/CollocationExtraction/src/resources/smallInput.txt");
        Defns.uploadFileToS3(Defns.PROJECT_NAME, Defns.JAR_NAME + ".jar", "C:/Users/יעל/מבוזרות/CollocationExtraction/target/CollocationExtraction-Jar-jar-with-dependencies.jar");
        /*for(int i = 0 ; i < Defns.Steps_Names.length ; i++){
            Defns.uploadFileToS3(Defns.PROJECT_NAME, Defns.Steps_Names[i] + ".jar", "C:/Users/יעל/מבוזרות/CollocationExtraction/out/artifacts/" + Defns.Steps_Names[i] + "_jar/" + Defns.Steps_Names[i] + ".jar");
        }*/
        // TODO: specify un the README file to change the input and class jars paths (or make it a comment)

        /*
         * Steps: (for each decade)
         * 1. Calc Cw1w2 and N 
         *  (key: lineID, value: line) --> 
         *      (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade#null#N, value: N)
         * 2. Calc Cw1 as the first word 
         *  (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade#null#N, value: N) --> 
         *      (key: decade#w1w2#Cw1, value: Cw1 ) 
         * 3. Calc Cw2 as the second word
         *  (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade#null#N, value: N) -->
         *    (key: decade#w1w2#Cw2, value: Cw2 )
         * 4. Calc Npmi = PMIw1w2 / (-log[Pw1w2])
         *      PMIw1w2 = log(Cw1w2) + log(N) - log(Cw1) - log(Cw2) ; Pw1w2 = Cw1w2 / N 
         *   (key: decade#w1w2#Cw1w2, value: Cw1w2) , (key: decade#null#N, value: N)
         *      (key: decade#w1w2#Cw1, value: Cw1 ) , (key: decade#w1w2#Cw2, value: Cw2 ) --> 
         *         (key: decade#w1w2#Npmi, value: Npmiw1w2) , (key: decade#null#NpmisSum, value: NpmisSum)
         * 5. Filter collocations with [(Npmi / SumNpmis) >= relMinNpmi] or [Npmi >= minNpmi], and sort by Npmi descending
         *      (key: decade#w1w2#Npmi, value: Npmiw1w2) , (key: decade#w1w2#SumNpmis, value: SumNpmis) --> 
         *         (key: decade#w1w2#Npmi, value: Npmiw1w2) filtered and sorted
         */

        //BasicConfigurator.configure(); // Add a ConsoleAppender that uses PatternLayout using the PatternLayout.TTCC_CONVERSION_PATTERN and prints to System.out to the root category.
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withRegion(Defns.regions).build();
        //System.out.println(mapReduce.listClusters());

        List<HadoopJarStepConfig> hadoopJarStepConfigs = new ArrayList<HadoopJarStepConfig>();
        for(int i = 0 ; i < Defns.Steps_Names.length ; i++) { 
            hadoopJarStepConfigs.add(new HadoopJarStepConfig()
                    .withJar(Defns.JAR_PATH) 
                    .withMainClass(Defns.Steps_Names[i])
                    .withArgs(Defns.getStepArgs(i)));
            System.out.println("Created jar step config for " + Defns.Steps_Names[i]);
        }

        List<StepConfig> stepConfigs = new ArrayList<StepConfig>();
        for(int i = 0 ; i < Defns.Steps_Names.length ; i++) { 
            stepConfigs.add(new StepConfig()
                    .withName(Defns.Steps_Names[i]) 
                    .withHadoopJarStep(hadoopJarStepConfigs.get(i))
                    .withActionOnFailure(Defns.TERMINATE_JOB_FLOW_MESSAGE));
            System.out.println("Created step config for " + Defns.Steps_Names[i]);
        }

    
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(Defns.instanceCount)
                .withMasterInstanceType(InstanceType.M4Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M4Xlarge.toString())
                .withHadoopVersion(Defns.HADOOP_VER)
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType(Defns.placementRegion));
        System.out.println("Created instances config.");

        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName(Defns.PROJECT_NAME)
                .withInstances(instances)
                .withSteps(stepConfigs)
                .withLogUri(Defns.Logs_URI)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");;
        System.out.println("Created run flow request.");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);

    }
    
}
