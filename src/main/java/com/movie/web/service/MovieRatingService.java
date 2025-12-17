package com.movie.web.service;

import com.movie.entity.MovieRating;
import com.movie.util.HBaseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 电影评分服务类
 */
@Service
public class MovieRatingService {
    
    private static final Logger logger = LoggerFactory.getLogger(MovieRatingService.class);  // 日志对象
    
    /**
     * 获取所有电影评分
     */
    public List<MovieRating> getAllRatings() {
        try {
            return HBaseUtil.scanAllMovieRatings();  // 调用HBase工具类获取所有评分
        } catch (IOException e) {
            logger.error("Error fetching all ratings", e);  // 记录错误日志
            return new ArrayList<>();
        }
    }
    
    /**
     * 获取单个电影评分
     */
    public MovieRating getMovieRating(String movieId) {
        try {
            return HBaseUtil.getMovieRating(movieId);
        } catch (IOException e) {
            logger.error("Error fetching rating for movie " + movieId, e);
            return null;
        }
    }
    
    /**
     * 获取热门电影（最近10分钟评分最多的Top10）
     * 通过最后更新时间筛选
     */
    public List<MovieRating> getHotMovies() {
        try {
            long tenMinutesAgo = System.currentTimeMillis() - 10 * 60 * 1000;
            
            // 获取所有电影评分
            List<MovieRating> allRatings = HBaseUtil.scanAllMovieRatings();
            
            // 筛选最近10分钟更新的电影，按评分次数排序
            return allRatings.stream()
                    .filter(r -> r.getLastUpdateTime() >= tenMinutesAgo)
                    .sorted((r1, r2) -> Long.compare(r2.getRatingCount(), r1.getRatingCount()))
                    .limit(10)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            logger.error("Error fetching hot movies", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 获取统计信息
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        
        try {
            List<MovieRating> allRatings = HBaseUtil.scanAllMovieRatings();
            stats.put("totalMovies", allRatings.size());
            
            long totalRatings = allRatings.stream()
                    .mapToLong(MovieRating::getRatingCount)
                    .sum();
            stats.put("totalRatings", totalRatings);
            
            double avgRating = allRatings.stream()
                    .mapToDouble(MovieRating::getAvgRating)
                    .average()
                    .orElse(0.0);
            stats.put("avgRating", Math.round(avgRating * 100.0) / 100.0);
            
            // 最近10分钟评分数
            long tenMinutesAgo = System.currentTimeMillis() - 10 * 60 * 1000;
            long recentCount = allRatings.stream()
                    .filter(r -> r.getLastUpdateTime() >= tenMinutesAgo)
                    .count();
            stats.put("recentRatings", recentCount);
            
        } catch (IOException e) {
            logger.error("Error fetching stats", e);
        }
        
        return stats;
    }
    
    /**
     * 通过标题模糊搜索电影
     */
    public List<MovieRating> searchByTitle(String keyword) {
        try {
            return HBaseUtil.searchByTitle(keyword);
        } catch (IOException e) {
            logger.error("Error searching movies by title: " + keyword, e);
            return new ArrayList<>();
        }
    }
}
