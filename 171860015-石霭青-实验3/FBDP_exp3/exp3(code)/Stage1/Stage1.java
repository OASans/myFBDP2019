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

    public static class Stage11Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {

            if (!line.toString().equals("")) {
                User user_record = new User(line.toString());
                context.write(new Text(user_record.getProvince()), new IntWritable(user_record.getItem_id()));
            }

        }

    }

    public static class Stage12Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            if (!line.toString().equals("")) {
                User user_record = new User(line.toString());
                if (user_record.getAction() == 2) {
                    context.write(new Text(user_record.getProvince()), new IntWritable(user_record.getItem_id()));
                }
            }
        }
    }

    public static class Stage1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        TreeMap treemap = new TreeMap<String, Integer>();
        ValueComparator bvc = new ValueComparator(treemap);
        TreeMap sortedmap = new TreeMap<String, Integer>(bvc);

        Text provinceName = new Text();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            provinceName = key;
            for (IntWritable i : values){

                if(treemap.get(i+"")!=null) {
                    int value = Integer.parseInt(treemap.get(i+"").toString());
                    treemap.put(i+"", value+1);

                } else {
                    treemap.put(i+"", 1);
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int topn = conf.getInt("top.n", 10);

            sortedmap.putAll(treemap);
            Set<Map.Entry<String, Integer>> entrySet = sortedmap.entrySet();
            int i = 0;

            context.write(provinceName, null);
            for (Map.Entry<String, Integer> entry : entrySet) {
                context.write(new Text((entry.getKey())), new IntWritable(entry.getValue()));
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
         //job.setMapperClass(Stage11Mapper.class);
        // 阶段一任务2的Mapper
        job.setMapperClass(Stage12Mapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(Stage1Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(40);

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
