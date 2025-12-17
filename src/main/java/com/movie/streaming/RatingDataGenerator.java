package com.movie.streaming;

import com.movie.entity.Rating;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * 实时评分数据生成器
 * 模拟用户对热门电影的实时评分
 */
public class RatingDataGenerator {
    
    // 热门电影ID列表
    private static final List<String> POPULAR_MOVIES = Arrays.asList(
        "1", "6", "47", "50", "110", "260", "318", "356", "480",
        "527", "593", "1196", "1210", "1270", "2571", "2858", 
        "4993", "5952", "79132", "168252"
    );
    
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting Rating Data Generator...");
        
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        System.out.println("Kafka bootstrap servers: " + bootstrapServers);
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Random random = new Random();
        
        System.out.println("Generating ratings... (Press Ctrl+C to stop)");
        
        try {
            while (true) {
                // 随机选择电影
                String movieId = POPULAR_MOVIES.get(random.nextInt(POPULAR_MOVIES.size()));
                
                // 生成随机评分 (0.5 - 5.0, 步长0.5)
                double rating = (random.nextInt(10) + 1) * 0.5;
                
                // 生成随机用户ID
                String userId = String.valueOf(random.nextInt(10000) + 1);
                
                Rating ratingObj = new Rating(userId, movieId, rating, System.currentTimeMillis());
                
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "movie-ratings", movieId, ratingObj.toJson()
                );
                
                producer.send(record);
                
                // 每秒生成1条
                Thread.sleep(1000);
            }
        } finally {
            producer.close();
        }
    }
}
