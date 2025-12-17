package com.movie.entity;

import java.io.Serializable;

/**
 * 电影评分实体类 - 用于HBase存储和前端展示
 */
public class MovieRating implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String movieId;        // 电影ID
    private String title;          // 电影标题
    private double avgRating;      // 平均评分
    private long ratingCount;      // 评分次数
    private long lastUpdateTime;   // 最后更新时间
    
    public MovieRating() {
    }
    
    public MovieRating(String movieId, String title, double avgRating, long ratingCount, long lastUpdateTime) {
        this.movieId = movieId;
        this.title = title;
        this.avgRating = avgRating;
        this.ratingCount = ratingCount;
        this.lastUpdateTime = lastUpdateTime;
    }
    
    // Getters and Setters
    public String getMovieId() {
        return movieId;
    }
    
    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public double getAvgRating() {
        return avgRating;
    }
    
    public void setAvgRating(double avgRating) {
        this.avgRating = avgRating;
    }
    
    public long getRatingCount() {
        return ratingCount;
    }
    
    public void setRatingCount(long ratingCount) {
        this.ratingCount = ratingCount;
    }
    
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }
    
    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
    
    @Override
    public String toString() {
        return "MovieRating{" +
                "movieId='" + movieId + '\'' +
                ", title='" + title + '\'' +
                ", avgRating=" + avgRating +
                ", ratingCount=" + ratingCount +
                ", lastUpdateTime=" + lastUpdateTime +
                '}';
    }
}
