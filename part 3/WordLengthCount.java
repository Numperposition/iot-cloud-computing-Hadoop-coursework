import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordLengthCount {
 public static void runJob(String[] input, String output) throws Exception {
   Configuration conf = new Configuration();
   Job job = new Job(conf);

   job.setJarByClass(WordLengthCount.class);
   job.setMapperClass(TokenToLengthMapper.class);//Sets mapper to your new Mapper class
   job.setReducerClass(IntSumReducer.class); //Sets reducer to your IntSumReducer class
   job.setMapOutputKeyClass(IntWritable.class);
   job.setMapOutputValueClass(IntWritable.class); //Sets the key and value format (IntWritable)

   Path outputPath = new Path(output);
   FileInputFormat.setInputPaths(job, StringUtils.join(input, ",")); //the input should be set to the out folder from Part 2!
   FileOutputFormat.setOutputPath(job, outputPath);
   outputPath.getFileSystem(conf).delete(outputPath,true);
   job.waitForCompletion(true);
 }

 public static void main(String[] args) throws Exception {
   runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]); //reads input/output folder from command line
 }
}
