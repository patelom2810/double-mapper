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

public class dmapq4 {

    // Mapper 1 - Daily Temperature
    public static class DailyTempMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Date,Month,Temperature
            String[] data = value.toString().split(",");
            String month = data[1];
            String temp = data[2];

            context.write(new Text(month), new Text(temp));
        }
    }

    // Mapper 2 - Monthly Temperature
    public static class MonthlyTempMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Month,Temperature
            String[] data = value.toString().split(",");
            context.write(new Text(data[0]), new Text(data[1]));
        }
    }

    // Reducer - Average Calculation
    public static class AvgTempReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;
            }

            double avg = sum / count;
         // Format the average temperature to 2 decimal places before writing output
            context.write(key, new Text(String.format("%.2f", avg)));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Temperature Data Analysis");

        job.setJarByClass(dmapq4.class);

        Path dailyInput = new Path(args[0]);
        Path monthlyInput = new Path(args[1]);
        Path output = new Path(args[2]);

        MultipleInputs.addInputPath(job, dailyInput,
                TextInputFormat.class, DailyTempMapper.class);
        MultipleInputs.addInputPath(job, monthlyInput,
                TextInputFormat.class, MonthlyTempMapper.class);

        job.setReducerClass(AvgTempReducer.class);

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

