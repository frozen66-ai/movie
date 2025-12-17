package com.movie.mapreduce;

import com.movie.entity.MovieRating;
import com.movie.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * MapReduce作业：计算电影评分并保存到HBase
 * 输出数据量很小（只有计算结果），适合HBase存储
 */
public class MovieRatingJob {
    
    /**
     * Mapper: 读取评分数据
     */
    //处理历史评分数据，计算电影的平均评分和评分次数，并将结果存储到 HBase   输入：原始评分数据（userId,movieId,rating,timestamp）
    //输出：(movieId, rating) 键值对
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text movieId = new Text();
        private DoubleWritable rating = new DoubleWritable();
        
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            // 跳过header
            if (key.get() == 0 && value.toString().startsWith("userId")) {
                return;
            }
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            if (fields.length >= 3) {
                try {
                    movieId.set(fields[1]);  // 电影ID作为key
                    rating.set(Double.parseDouble(fields[2]));
                    context.write(movieId, rating);
                } catch (NumberFormatException e) {
                    // 忽略格式错误的行
                }
            }
        }
    }
    
    /**
     * Reducer: 计算平均评分并保存到HBase
     */
    public static class RatingReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Map<String, String> movieTitles = new HashMap<>();
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // 读取电影标题信息
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (java.io.BufferedReader reader = new java.io.BufferedReader(
                        new java.io.InputStreamReader(
                                new java.io.FileInputStream("movies.csv")))) {
                    String line;
                    boolean isHeader = true;
                    while ((line = reader.readLine()) != null) {
                        if (isHeader) {
                            isHeader = false;
                            continue;
                        }
                        String[] fields = line.split(",");
                        if (fields.length >= 2) {
                            movieTitles.put(fields[0], fields[1]);
                        }
                    }
                }
            }
            
            // 创建HBase表
            try {
                HBaseUtil.createTable();
            } catch (Exception e) {
                System.err.println("Error creating HBase table: " + e.getMessage());
            }
        }


        //输入：(movieId, [rating1, rating2, ...])
        //处理：计算总评分、评分次数和平均评分
        //输出：将结果封装为MovieRating对象并存储到 HBase
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            double sum = 0.0;
            long count = 0;
            
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            
            double avgRating = sum / count;
            String movieId = key.toString();
            String title = movieTitles.getOrDefault(movieId, "Movie " + movieId);
            
            // 保存到HBase
            MovieRating rating = new MovieRating(
                movieId, title, avgRating, count, System.currentTimeMillis()
            );
            
            try {
                HBaseUtil.saveOrUpdateMovieRating(rating);
            } catch (IOException e) {
                System.err.println("Error saving to HBase: " + e.getMessage());
            }
            
            // 输出到HDFS（备份）
            context.write(key, new Text(String.format("%.2f,%d", avgRating, count)));
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("All ratings saved to HBase successfully!");
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MovieRatingJob <ratings_input> <movies_input> <output>");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Calculation");
        
        job.setJarByClass(MovieRatingJob.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        // 添加电影数据作为缓存文件
        job.addCacheFile(new URI(args[1] + "#movies.csv"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
