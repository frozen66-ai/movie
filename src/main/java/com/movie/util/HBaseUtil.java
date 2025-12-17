package com.movie.util;

import com.movie.entity.MovieRating;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HBase操作工具类
 */
public class HBaseUtil {
    private static final String TABLE_NAME = "movie_ratings";  // HBase表名
    private static final String COLUMN_FAMILY = "rating";  // 列族名
    private static Connection connection;  // HBase连接
    // 静态初始化块，创建HBase连接
    static {
        try {
            Configuration conf = HBaseConfiguration.create(); // 创建HBase配置
            String zkQuorum = System.getProperty("hbase.zookeeper.quorum", "localhost"); // ZooKeeper地址
            conf.set("hbase.zookeeper.quorum", zkQuorum);
            conf.set("hbase.zookeeper.property.clientPort", "2181"); // ZooKeeper端口
            connection = ConnectionFactory.createConnection(conf); // 创建连接
            System.out.println("HBase connection created successfully");
        } catch (IOException e) {
            System.err.println("Failed to create HBase connection: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * 创建表
     */
    public static void createTable() throws IOException {
        Admin admin = connection.getAdmin(); // 获取管理员对象
        TableName tableName = TableName.valueOf(TABLE_NAME); // 表名
        
        if (!admin.tableExists(tableName)) {  // 如果表不存在
            TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
            // 创建列族 HBase 是列式存储数据库，表的结构是「表（Table）→ 行（Row）→ 列族（Column Family）→ 列限定符（Column Qualifier）→ 值（Value）」
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder //列族描述符构建器
                    .newBuilder(Bytes.toBytes(COLUMN_FAMILY)) //初始化构建器
                    .build();//生成列族描述符对象
            builder.setColumnFamily(cf);
            admin.createTable(builder.build()); // 创建表
            System.out.println("Table " + TABLE_NAME + " created successfully");
        } else {
            System.out.println("Table " + TABLE_NAME + " already exists");
        }
        admin.close(); // 关闭管理员
    }
    
    /**
     * 保存或更新电影评分
     */
    //对应 MapReduce 离线计算结果写入、Spark Streaming 实时评分更新，核心方法 saveOrUpdateMovieRating：
    public static void saveOrUpdateMovieRating(MovieRating rating) throws IOException {
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));  // 获取表对象
        Put put = new Put(Bytes.toBytes(rating.getMovieId()));  // 创建Put对象，行键为movieId
        // 添加各列数据
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("title"), 
                      Bytes.toBytes(rating.getTitle()));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_rating"), 
                      Bytes.toBytes(rating.getAvgRating()));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("rating_count"), 
                      Bytes.toBytes(rating.getRatingCount()));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("update_time"), 
                      Bytes.toBytes(rating.getLastUpdateTime()));
        
        table.put(put); // 执行插入/更新
        table.close(); // 关闭表
    }
    
    /**
     * 获取单个电影评分
     */
    //数据查询（支撑前端查询 / 实时更新前的历史数据读取）
    //核心方法 getMovieRating：根据 movieId 查询电影评分统计数据，供 Spark Streaming 实时更新时读取历史数据：
    public static MovieRating getMovieRating(String movieId) throws IOException {
        // 1. 获取表连接
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        // 2. 构建 Get 对象（HBase 单条查询的核心载体，指定行键）
        Get get = new Get(Bytes.toBytes(movieId));
        Result result = table.get(get);
        table.close();
        // 5. 判断是否查询到数据（行键不存在则返回 null）
        if (result.isEmpty()) {
            return null;
        }
        // 6. 解析 Result 中的字节数组，转换为 MovieRating 对象
        MovieRating rating = new MovieRating();
        rating.setMovieId(movieId);
        rating.setTitle(Bytes.toString(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("title"))));
        rating.setAvgRating(Bytes.toDouble(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_rating"))));
        rating.setRatingCount(Bytes.toLong(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("rating_count"))));
        rating.setLastUpdateTime(Bytes.toLong(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("update_time"))));
        
        return rating;
    }
    
    /**
     * 扫描所有电影评分
     */
    public static List<MovieRating> scanAllMovieRatings() throws IOException {
        List<MovieRating> ratings = new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            try {
                MovieRating rating = new MovieRating();
                rating.setMovieId(Bytes.toString(result.getRow()));
                
                byte[] titleBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("title"));
                byte[] avgRatingBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("avg_rating"));
                byte[] countBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("rating_count"));
                byte[] timeBytes = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("update_time"));
                
                if (titleBytes != null) {
                    rating.setTitle(Bytes.toString(titleBytes));
                } else {
                    rating.setTitle("Unknown");
                }
                
                if (avgRatingBytes != null && avgRatingBytes.length == 8) {
                    rating.setAvgRating(Bytes.toDouble(avgRatingBytes));
                } else {
                    rating.setAvgRating(0.0);
                }
                
                if (countBytes != null && countBytes.length == 8) {
                    rating.setRatingCount(Bytes.toLong(countBytes));
                } else {
                    rating.setRatingCount(0);
                }
                
                if (timeBytes != null && timeBytes.length == 8) {
                    rating.setLastUpdateTime(Bytes.toLong(timeBytes));
                } else {
                    rating.setLastUpdateTime(System.currentTimeMillis());
                }
                
                ratings.add(rating);
            } catch (Exception e) {
                // 跳过损坏的记录
                System.err.println("Skipping corrupted record: " + e.getMessage());
            }
        }
        
        scanner.close();
        table.close();
        return ratings;
    }
    
    /**
     * 通过标题模糊查询电影
     */
    public static List<MovieRating> searchByTitle(String keyword) throws IOException {
        List<MovieRating> results = new ArrayList<>();
        List<MovieRating> allRatings = scanAllMovieRatings();
        
        String lowerKeyword = keyword.toLowerCase();
        for (MovieRating rating : allRatings) {
            if (rating.getTitle() != null && 
                rating.getTitle().toLowerCase().contains(lowerKeyword)) {
                results.add(rating);
            }
        }
        
        return results;
    }
    
    /**
     * 关闭连接
     */
    public static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
