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


public class dmapq10 {


    // Mapper 1 : Processes Social Media Posts

    public static class PostMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        /*
         * Runs for each line of social media post file
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format:
            // U001,P001,I love this product,Positive
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> User ID
            // Value -> Sentiment of the post
            context.write(
                new Text(data[0]),
                new Text("SENT:" + data[3])
            );
        }
    }


    // Mapper 2 : Processes User Demographic Data

    public static class UserMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        /*
         * Runs for each line of user data file
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format:
            // U001,Om Patel,22,Male,Ahmedabad
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> User ID
            // Value -> User details
            context.write(
                new Text(data[0]),
                new Text("USER:" + data[1] + "," + data[2] + "," + data[3] + "," + data[4])
            );
        }
    }


    public static class SentimentReducer
            extends Reducer<Text, Text, Text, Text> {

        /*
         * Runs once for each User ID
         * Combines user details with sentiments of posts
         */
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String name = "";
            String age = "";
            String gender = "";
            String city = "";
            String sentiments = "";

            // Loop through all values of a user
            for (Text val : values) {
                String v = val.toString();

                // Check if value contains user information
                if (v.startsWith("USER:")) {

                    // Extract user details
                    String[] u = v.replace("USER:", "").split(",");
                    name = u[0];
                    age = u[1];
                    gender = u[2];
                    city = u[3];

                } else {

                    // Collect sentiments from posts
                    sentiments += v.replace("SENT:", "") + ", ";
                }
            }

            // Output final correlated result
            // UserID -> Name | Age | Gender | City | Sentiments
            context.write(
                key,
                new Text(name + " | " + age + " | " + gender + " | " + city + " | " + sentiments)
            );
        }
    }


    public static void main(String[] args) throws Exception {

        // Create configuration
        Configuration conf = new Configuration();

        // Create MapReduce job
        Job job = Job.getInstance(conf, "Social Media Sentiment Analysis");

        // Set main class
        job.setJarByClass(dmapq10.class);

        // Add social media post file with PostMapper
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, PostMapper.class);

        // Add user data file with UserMapper
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, UserMapper.class);

        // Set reducer
        job.setReducerClass(SentimentReducer.class);

        // Set output key and value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Run job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
