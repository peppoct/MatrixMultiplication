import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

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

    public static class MatrixReducer extends Reducer<Text, Text, Text,LongWritable> {

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


        }
    }
}
