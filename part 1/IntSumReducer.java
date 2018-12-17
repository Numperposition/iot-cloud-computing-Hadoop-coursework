import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //The above <Test, IntWritable, Text IntWritable> tells Java the type of <KEY-IN, VALUE-IN, KEY-OUT, VALUE-OUT>.
    private IntWritable result = new IntWritable(); //a int value in Hadoop - where sum result should be stored

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //This reduce method will be called for each key (i.e. each word). Values will contain all the values that have been recorded for that word...
        int sum = 0; 

        for (IntWritable value : values) {
            sum += value.get();
            //complete code here! HINT: You need to go through each value and sum up the result!
        }

        result.set(sum); //sets result to value of sum
        context.write(key, result);
        //complete code here! HINT: You need to emit the outputs!

    }

}
