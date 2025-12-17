package com.movie.entity;

import com.google.gson.Gson;
import java.io.Serializable;

/**
 * 单条用户评分实体 - 用于Kafka消息传输
 */
public class Rating implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String userId;
    private String movieId;
    private double rating;
    private long timestamp;
    
    public Rating() {
    }
    
    public Rating(String userId, String movieId, double rating, long timestamp) {
        this.userId = userId;
        this.movieId = movieId;
        this.rating = rating;
        this.timestamp = timestamp;
    }

    // 以下是各字段的getter和setter方法

    //getUserId() 用于获取 userId 的值，setUserId(String userId) 用于设置 userId 的值
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getMovieId() {
        return movieId;
    }
    
    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }
    
    public double getRating() {
        return rating;
    }
    
    public void setRating(double rating) {
        this.rating = rating;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    /**
     * 转换为JSON字符串
     */
    public String toJson() {
        return new Gson().toJson(this);
    }
    
    /**
     * 从JSON字符串解析
     */
    public static Rating fromJson(String json) {
        return new Gson().fromJson(json, Rating.class);
    }  // 从JSON解析为对象

    // 重写toString方法，方便打印调试
    @Override
    public String toString() {
        return "Rating{" +
                "userId='" + userId + '\'' +
                ", movieId='" + movieId + '\'' +
                ", rating=" + rating +
                ", timestamp=" + timestamp +
                '}';
    }
}
