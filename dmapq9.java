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


public class dmapq9 {

    // Mapper 1 : Reads Website Clickstream Data

    public static class ClickMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        /*
         * This method runs for each line of clickstream file
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format:
            // U001,2026-02-10 10:01:10,/home,click
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> User ID
            // Value -> Page visited by the user
            context.write(
                new Text(data[0]),
                new Text("PAGE:" + data[2])
            );
        }
    }

    // Mapper 2 : Reads User Demographic Data

    public static class UserMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        /*
         * This method runs for each line of demographic file
         */
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format:
            // U001,Om Patel,22,Male,Ahmedabad
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> User ID
            // Value -> User details (Name, Age, Gender, City)
            context.write(
                new Text(data[0]),
                new Text("USER:" + data[1] + "," + data[2] + "," + data[3] + "," + data[4])
            );
        }
    }


    public static class ClickReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Variables to store user information
            String name = "";
            String age = "";
            String gender = "";
            String city = "";

            // Variable to store all pages visited by the user
            String pages = "";

            // Loop through all values for one User ID
            for (Text val : values) {
                String v = val.toString();

                // Check if value contains user details
                if (v.startsWith("USER:")) {

                    // Remove USER: and split details
                    String[] u = v.replace("USER:", "").split(",");

                    // Assign user information
                    name = u[0];
                    age = u[1];
                    gender = u[2];
                    city = u[3];

                } else {

                    // Value contains page visited
                    pages += v.replace("PAGE:", "") + ", ";
                }
            }

            // Write final output:
            // UserID -> Name | Age | Gender | City | Pages Visited
            context.write(
                key,
                new Text(name + " | " + age + " | " + gender + " | " + city + " | " + pages)
            );
        }
    }

    public static void main(String[] args) throws Exception {

        // Create Hadoop configuration
        Configuration conf = new Configuration();

        // Create a new MapReduce job
        Job job = Job.getInstance(conf, "Website Clickstream Analysis");

        // Set main class
        job.setJarByClass(dmapq9.class);

        // Add clickstream input file with ClickMapper
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, ClickMapper.class);

        // Add demographic input file with UserMapper
        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, UserMapper.class);

        // Set reducer class
        job.setReducerClass(ClickReducer.class);

        // Set mapper output key and value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Set final output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Run the job and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

