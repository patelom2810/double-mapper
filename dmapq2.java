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

public class dmapq2 {

    // Mapper 1 - Login Data
    public static class LoginMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] data = value.toString().split(",");
            // UserID,LoginDate
            context.write(new Text(data[0]), new Text("LOGIN:" + data[1]));
        }
    }

    // Mapper 2 - Activity Data
    public static class ActivityMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] data = value.toString().split(",");
            // UserID,ActivityType
            context.write(new Text(data[0]), new Text("ACTIVITY:" + data[1]));
        }
    }

    // Reducer
    public static class UserReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String loginDate = "";
            String activities = "";

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("LOGIN:")) {
                    loginDate = v.replace("LOGIN:", "");
                } else if (v.startsWith("ACTIVITY:")) {
                    activities += v.replace("ACTIVITY:", "") + ", ";
                }
            }

            context.write(key, new Text(loginDate + "\t" + activities));
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Activity Analysis");

        job.setJarByClass(dmapq2.class);

        Path loginInput = new Path(args[0]);
        Path activityInput = new Path(args[1]);
        Path output = new Path(args[2]);

        MultipleInputs.addInputPath(job, loginInput,
                TextInputFormat.class, LoginMapper.class);
        MultipleInputs.addInputPath(job, activityInput,
                TextInputFormat.class, ActivityMapper.class);

        job.setReducerClass(UserReducer.class);

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
