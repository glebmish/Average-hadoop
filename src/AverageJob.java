import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class AverageJob {

    public static class AverageMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Text text = new Text();
        private final IntWritable val = new IntWritable();

        protected void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());

            while (tokenizer.hasMoreTokens()) {
                val.set(Integer.parseInt(tokenizer.nextToken()));
                context.write(text, val);
            }
        }
    }

    public static class AverageReducer extends Reducer<Text, IntWritable, NullWritable, DoubleWritable> {
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context)
            throws IOException, InterruptedException {
            double sum = 0;
            int ct = 0;
            for (IntWritable value : values) {
                sum += value.get();
                ct++;
            }
            context.write(key, new DoubleWritable(sum / ct));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average");
        job.setJarByClass(AverageMapper.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);

        job.setMapperClass(AverageMapper.class);
        job.setReducerClass(AverageReducer.class);

        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.waitForCompletion(true);
    }
}
