package avgmarks;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class dmapq3 {

    // Mapper 1 - Online Sales
    public static class OnlineSalesMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] data = value.toString().split(",");
            // ProductID,Quantity,Revenue
            context.write(new Text(data[0]),
                    new Text(data[1] + "|" + data[2]));
        }
    }

    // Mapper 2 - Store Sales
    public static class StoreSalesMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] data = value.toString().split(",");
            // ProductID,Quantity,Revenue
            context.write(new Text(data[0]),
                    new Text(data[1] + "|" + data[2]));
        }
    }

    // Reducer
    public static class SalesReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int totalQty = 0;
            int totalRevenue = 0;

            for (Text val : values) {
                String[] parts = val.toString().split("\\|");
                totalQty += Integer.parseInt(parts[0]);
                totalRevenue += Integer.parseInt(parts[1]);
            }

            context.write(key,
                    new Text(totalQty + "\t" + totalRevenue));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sales Data Aggregation");

        job.setJarByClass(dmapq3.class);

        Path onlineInput = new Path(args[0]);
        Path storeInput = new Path(args[1]);
        Path output = new Path(args[2]);

        MultipleInputs.addInputPath(job, onlineInput,
                TextInputFormat.class, OnlineSalesMapper.class);
        MultipleInputs.addInputPath(job, storeInput,
                TextInputFormat.class, StoreSalesMapper.class);

        job.setReducerClass(SalesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);

        output.getFileSystem(conf).delete(output, true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

