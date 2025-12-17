package com.movie.web.controller;

import com.movie.entity.MovieRating;
import com.movie.web.service.MovieRatingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 电影评分REST控制器
 */
@RestController
@RequestMapping("/api/movies")
@CrossOrigin(origins = "*")
public class MovieController {
    
    @Autowired
    private MovieRatingService ratingService;
    
    /**
     * 获取所有电影评分
     */
    @GetMapping("/ratings")
    public List<MovieRating> getAllRatings() {
        return ratingService.getAllRatings();
    }
    
    /**
     * 获取单个电影评分
     */
    @GetMapping("/{movieId}")
    public MovieRating getMovieRating(@PathVariable String movieId) {
        return ratingService.getMovieRating(movieId);
    }
    
    /**
     * 获取热门电影 (最近10分钟评分最多的Top10)
     */
    @GetMapping("/hot")
    public List<MovieRating> getHotMovies() {
        return ratingService.getHotMovies();
    }
    
    /**
     * 获取统计信息
     */
    @GetMapping("/stats")
    public Map<String, Object> getStats() {
        return ratingService.getStats();
    }
    
    /**
     * 通过标题模糊搜索电影
     */
    @GetMapping("/search")
    public List<MovieRating> searchByTitle(@RequestParam String title) {
        return ratingService.searchByTitle(title);
    }
}
