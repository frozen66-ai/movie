package com.movie.streaming;

import com.movie.entity.MovieRating;
import com.movie.entity.Rating;
import com.movie.util.HBaseUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;

/**
 * Spark Streaming作业：实时处理评分数据
 * 1. 实时更新HBase中的电影评分
 * 2. 计算最近10分钟的热门电影Top10
 */
public class MovieRatingStreamingJob {
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Movie Rating Streaming Job...");
//创建 “流处理上下文”—— 开启流处理能力
        // 配置Spark 本地模式2个线程（1个接收数据，1个处理数据）
        SparkConf conf = new SparkConf()
            .setAppName("MovieRatingStreaming")  // 设置应用名称
            .setMaster("local[2]");   // 本地运行模式，2个线程

        // 创建Streaming上下文，批处理间隔5秒
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        // 设置检查点目录，用于状态管理
        jssc.checkpoint("hdfs://node1:8020/tmp/spark-streaming-checkpoint");


        // Kafka参数
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka服务器地址
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // 键反序列化器
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class); // 值反序列化器
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "movie-rating-group"); // 消费者组ID
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // 从最新位置开始消费

        // 要消费的Kafka主题
        Collection<String> topics = Collections.singletonList("movie-ratings");
        
        // 创建Kafka数据流  通过 Kafka 直接读取数据  KafkaUtils：Spark Streaming 提供的操作 Kafka 的工具类 createDirectStream：创建 “直接连接模式” 的 Kafka 输入流 接收 3 个核心参数
        //消费 Kafka 中的实时评分数据，更新 HBase 中的电影评分，并计算最近 10 分钟的热门电影 Top10
        JavaInputDStream<ConsumerRecord<String, String>> stream =       //从 Kafka 主题movie-ratings消费实时评分数据
            KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),  // 数据分配策略
                ConsumerStrategies.Subscribe(topics, kafkaParams) // 订阅主题
            );
        
        // 解析评分数据
        JavaPairDStream<String, Rating> ratingStream = stream.mapToPair(record -> {
            Rating rating = Rating.fromJson(record.value());  // 从JSON解析评分
            return new Tuple2<>(rating.getMovieId(), rating);  // 返回(movieId, rating)
        });
        
        // 实时更新HBase
        ratingStream.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                String movieId = tuple._1();  // 获取电影ID
                Rating newRating = tuple._2();  // 获取新评分
                
                try {
                    // 从HBase读取当前评分
                    MovieRating current = HBaseUtil.getMovieRating(movieId);
                    
                    if (current != null) {
                        // 更新评分 计算新的平均评分和评分次数
                        long newCount = current.getRatingCount() + 1;
                        double newAvg = (current.getAvgRating() * current.getRatingCount() + newRating.getRating()) / newCount;
                        // 更新HBase
                        current.setAvgRating(newAvg);
                        current.setRatingCount(newCount);
                        current.setLastUpdateTime(System.currentTimeMillis());
                        
                        HBaseUtil.saveOrUpdateMovieRating(current);
                        System.out.println("Updated movie " + movieId + ": avg=" + String.format("%.2f", newAvg) + ", count=" + newCount);
                    }
                } catch (Exception e) {
                    System.err.println("Error updating HBase for movie " + movieId + ": " + e.getMessage());
                }
            });
        });
        
        // 10分钟窗口统计热门电影
        JavaPairDStream<String, Integer> movieCountWindow = ratingStream
            .mapToPair(tuple -> new Tuple2<>(tuple._1(), 1))  // 转换为(movieId, 1)
            .reduceByKeyAndWindow(
                Integer::sum,  // 求和函数
                Durations.minutes(10),  // 窗口大小10分钟
                Durations.seconds(5)  // 滑动间隔5秒
            );
        
        // 打印热门电影Top 10
        movieCountWindow.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                List<Tuple2<String, Integer>> allMovies = new ArrayList<>(rdd.collect());
                // 按评分次数降序排序
                allMovies.sort((a, b) -> Integer.compare(b._2, a._2));
                
                int topN = Math.min(10, allMovies.size());
                System.out.println("\n========== Top 10 Hot Movies (Last 10 Minutes) ==========");
                for (int i = 0; i < topN; i++) {
                    Tuple2<String, Integer> movie = allMovies.get(i);
                    try {
                        // 获取电影详细信息
                        MovieRating rating = HBaseUtil.getMovieRating(movie._1);
                        String title = rating != null ? rating.getTitle() : "Unknown";
                        double avgRating = rating != null ? rating.getAvgRating() : 0.0;
                        // 打印结果
                        System.out.println(String.format("%d. MovieID: %s, Title: %s, Rating Count: %d, Avg Rating: %.2f",
                                i + 1, movie._1, title, movie._2, avgRating));
                    } catch (Exception e) {
                        System.err.println("Error fetching movie info: " + e.getMessage());
                    }
                }
                System.out.println("=======================================================\n");
            }
        });
        
        System.out.println("Streaming job started. Waiting for data...");
        jssc.start();  // 启动流处理
        jssc.awaitTermination(); // 等待终止
    }
}
