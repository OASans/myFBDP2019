package exp3.Stage1;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import exp3.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Stage1 {

    public static class Stage11Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            if (!line.toString().equals("")) {
                User user_record = new User(line.toString());
                context.write(new IntWritable(user_record.getItem_id()), new IntWritable(1));
            }
        }
    }

    public static class Stage12Mapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            if (!line.toString().equals("")) {
                User user_record = new User(line.toString());
                if (user_record.getAction() == 2) {
                    context.write(new IntWritable(user_record.getItem_id()), new IntWritable(1));
                }
            }
        }
    }

    public static class Stage1Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        TreeMap treemap = new TreeMap<User, Object>();
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) {
                sum += i.get();
            }

            User item_result = new User();
            item_result.setItem_id(key.get());
            item_result.setAction(sum);
            treemap.put(item_result, null);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int topn = conf.getInt("top.n", 10);

            Set<Map.Entry<User, Object>> entrySet = treemap.entrySet();
            int i = 0;
            for (Map.Entry<User, Object> entry : entrySet) {
                context.write(new IntWritable(entry.getKey().getItem_id()), new IntWritable(entry.getKey().getAction()));
                i++;
                if (i==topn) {
                    return;
                }
            }
        }

    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Stage1");

        job.setJarByClass(Stage1.class);

        // 阶段一任务1的Mapper
        // job.setMapperClass(Stage11Mapper.class);
        // 阶段一任务2的Mapper
        job.setMapperClass(Stage12Mapper.class);

        job.setReducerClass(Stage1Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        if (b) {
            System.out.println("finished!");
        } else {
            System.out.println("fail!");
        }

    }
}
