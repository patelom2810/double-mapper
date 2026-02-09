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

public class dmapq7 {

    public static class CustomerMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format:
            // C001,Om Patel,22,Male,Ahmedabad
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> CustomerID
            // Value -> Customer details (Name, Age, Gender, City)
            context.write(
                new Text(data[0]),
                new Text("CUST:" + data[1] + "," + data[2] + "," + data[3] + "," + data[4])
            );
        }
    }

    // =================================================
    // Mapper 2 : Processes Purchase Transaction Data
    // =================================================
    public static class PurchaseMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Input format:
            // C001,P1001,Mobile,15000
            String[] data = value.toString().split(",");

            // Emit:
            // Key   -> CustomerID
            // Value -> Purchased product name
            context.write(
                new Text(data[0]),
                new Text("PROD:" + data[2])
            );
        }
    }


    public static class CustomerReducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Variables to store customer details
            String name = "";
            String age = "";
            String gender = "";
            String city = "";

            // Variable to store all purchased products
            String products = "";

            // Iterate through all values for a CustomerID
            for (Text val : values) {
                String v = val.toString();

                // Check if value contains customer details
                if (v.startsWith("CUST:")) {

                    // Extract customer information
                    String[] cust = v.replace("CUST:", "").split(",");
                    name = cust[0];
                    age = cust[1];
                    gender = cust[2];
                    city = cust[3];

                } 
                // Otherwise, value contains product information
                else {
                    products += v.replace("PROD:", "") + ", ";
                }
            }

            // Output format:
            // CustomerID -> Name | Age | Gender | City | Products
            context.write(
                key,
                new Text(name + " | " + age + " | " + gender + " | " + city + " | " + products)
            );
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Customer Purchase History");

        // Set main class
        job.setJarByClass(dmapq7.class);

        // Add two input files with two different mappers
        MultipleInputs.addInputPath(job, new Path(args[0]),
                TextInputFormat.class, CustomerMapper.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, PurchaseMapper.class);

        // Set reducer
        job.setReducerClass(CustomerReducer.class);

        // Set output key and value types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set output path
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Execute job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

