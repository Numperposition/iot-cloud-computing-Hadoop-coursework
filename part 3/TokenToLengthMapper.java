import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenToLengthMapper extends Mapper<Object, Text, IntWritable, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private IntWritable word_length = new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
       //This map method is called for every line in the text file
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while(itr.hasMoreTokens())
        {
           String[] components = itr.nextToken().toString().split("\t");
           word_length.set(components[0].length()); 
           context.write(word_length, one);
        }
     
        
      }
    
}
