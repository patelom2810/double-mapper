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

public class dmapq8 {

    // =====================================
    // Mapper 1 : Reads User Rating Data
    // =====================================
    public static class RatingMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format: U001,M001,4
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> MovieID
            // Value -> Rating
            context.write(
                new Text(data[1]),
                new Text("R," + data[2])
            );
        }
    }

    // =====================================
    // Mapper 2 : Reads Movie Genre Data
    // =====================================
    public static class GenreMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format: M001,Action
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> MovieID
            // Value -> Genre
            context.write(
                new Text(data[0]),
                new Text("G," + data[1])
            );
        }
    }

    // =====================================
    // Reducer : Simplest Version (NO cleanup)
    // =====================================
    public static class MovieReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Stores movie genre
            String genre = "";

            // Stores sum of ratings for this movie
            int sum = 0;

            // Stores count of ratings
            int count = 0;

            // Process one movie
            for (Text val : values) {
                String v = val.toString();

                // If value contains genre
                if (v.startsWith("G,")) {

                    // Extract genre name
                    genre = v.split(",")[1];

                } else {

                    // Extract rating
                    sum += Integer.parseInt(v.split(",")[1]);
                    count++;
                }
            }

            // Calculate average rating for this movie
            double avg = (double) sum / count;

            // Output genre and average rating
            context.write(new Text(genre), new Text(avg + ""));
        }
    }

    // =====================================
    // Driver Class
    // =====================================
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Recommendation System");

        // Set main class
        job.setJarByClass(dmapq8.class);

        // Add two input files with two mappers
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, GenreMapper.class);

        // Set reducer
        job.setReducerClass(MovieReducer.class);

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
