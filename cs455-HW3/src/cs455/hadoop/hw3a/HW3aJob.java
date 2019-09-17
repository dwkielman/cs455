package cs455.hadoop.hw3a;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HW3aJob {
	
	public static void main(String[] args) {
		
		try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "HW3a");
            // Current class.
            job.setJarByClass(HW3aJob.class);
            // Combiner
            job.setCombinerClass(HW3aCombiner.class);
            // Reducer
            job.setReducerClass(HW3aReducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            Path p1 = new Path(args[0]);
            Path p2 = new Path(args[1]);
            
            MultipleInputs.addInputPath(job, p1, TextInputFormat.class, HW3aAnalysisMapper.class);
            MultipleInputs.addInputPath(job, p2, TextInputFormat.class, HW3aMetadataMapper.class);
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
		
	}

}
