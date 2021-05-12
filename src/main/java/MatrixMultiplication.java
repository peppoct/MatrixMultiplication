import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.awt.dnd.DragGestureRecognizer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class MatrixMultiplication {

    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text>{

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            int col_N = Integer.parseInt(conf.get("columns_N"));
            int row_M = Integer.parseInt(conf.get(("rows_M")));


            String[] values = value.toString().split(",");
            if (values[0].equals("M")){
                for (int i=0; i<col_N; i++){
                    outputKey.set(values[1]+","+i);
                    outputValue.set(values[0]+","+values[2]+","+values[3]);
                    context.write(outputKey, outputValue);
                }
            } else if (values[0].equals("N")){
                for (int j=0; j<row_M; j++){
                    outputKey.set(values[1]+","+j);
                    outputValue.set(values[0]+","+values[1]+","+values[3]);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            HashMap<Integer, Float> M = new HashMap<>();
            HashMap<Integer, Float> N = new HashMap<>();

            String[] riga;
            for (Text value : values){
                riga = value.toString().split(",");
                if (riga[0].equals("M")){
                    M.put(Integer.parseInt(riga[1]), Float.parseFloat(riga[2]));
                } else {
                    N.put(Integer.parseInt(riga[1]), Float.parseFloat(riga[2]));
                }
            }

            Configuration conf = context.getConfiguration();
            int col_M = Integer.parseInt(conf.get("columns_M"));
            float sum = (float) 0.0;
            float m, n;

            for (int j=0; j<col_M; j++){
                if (M.containsKey(j) && N.containsKey(j)){
                    m = M.get(j);
                    n = N.get(j);
                    sum += m*n;
                }
            }

            if (sum != 0.0){
                context.write(null, new Text(key.toString() + ","+ Float.toString(sum)));
            }

        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Error");
            System.exit(1);
        }

        System.out.println("args[0]: <input>"+otherArgs[0]);
        System.out.println("args[1]: <output>"+otherArgs[1]);

        Job job = Job.getInstance(conf, "MatrixMultiplication");
        job.getConfiguration().set("columns_N", "3");
        job.getConfiguration().set("rows_M", "3");
        job.getConfiguration().set("columns_M", "3");
        job.getConfiguration().set("rows_N", "3");

        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
