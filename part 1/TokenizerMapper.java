import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       //This map method is called for every line in the text file
        StringTokenizer itr = new StringTokenizer(value.toString(), "-- \t\n\r\f,.:;?![]()'\"");
        //The while loop iterates through each token/word in the string 
        while (itr.hasMoreTokens()) {
          data.set(itr.nextToken().toLowerCase()); //sets the value of data to the next word
          context.write(data, one); //writes the value 1 under the key. E.g. if the word were cat, it would write <cat,1>
        }
    }
}
