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

public class dmapq5 {

    public static class PriceMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input: StockSymbol,Date,Price
            String[] data = value.toString().split(",");

            // Emit: StockSymbol , P,Price
            context.write(
                    new Text(data[0]),
                    new Text("P," + data[2])
            );
        }
    }


    public static class VolumeMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input: StockSymbol,Date,Volume
            String[] data = value.toString().split(",");

            // Emit: StockSymbol , V,Volume
            context.write(
                    new Text(data[0]),
                    new Text("V," + data[2])
            );
        }
    }


 // Reducer class: processes all values related to one StockSymbol
    public static class EasyReducer
            extends Reducer<Text, Text, Text, Text> {

        // This method runs once for each StockSymbol
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Stores the sum of all stock prices
            double priceSum = 0;

            // Stores the sum of all trading volumes (not used in calculation here)
            double volumeSum = 0;

            // Counts how many price records are present
            int priceCount = 0;

            // Loop through all values of one stock symbol
            for (Text val : values) {

                // Split the value into type and number
                // Format: P,price  OR  V,volume
                String[] parts = val.toString().split(",");

                // If the record is a price
                if (parts[0].equals("P")) {

                    // Add price to total price
                    priceSum += Double.parseDouble(parts[1]);

                    // Increase price count
                    priceCount++;

                } else {

                    // Record is volume, add volume to total volume
                    volumeSum += Double.parseDouble(parts[1]);
                }
            }

            // Calculate simple average price
            double avgPrice = priceSum / priceCount;

            // Output the stock symbol and its average price
            context.write(key, new Text(avgPrice + ""));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Market Analysis - Easy");

        job.setJarByClass(dmapq5.class);

        // Two input files with two mappers
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, PriceMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, VolumeMapper.class);

        job.setReducerClass(EasyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
