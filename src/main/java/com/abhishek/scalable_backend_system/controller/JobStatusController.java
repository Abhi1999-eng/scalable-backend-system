package com.abhishek.scalable_backend_system.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/jobs")
public class JobStatusController {

    private final StringRedisTemplate redisTemplate;

    public JobStatusController(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/{jobId}")
    public Map<String, String> getStatus(@PathVariable int jobId) {

        String progress = redisTemplate.opsForValue().get("job:" + jobId + ":progress");
        String total = redisTemplate.opsForValue().get("job:" + jobId + ":total");

        Map<String, String> res = new HashMap<>();
        res.put("progress", progress == null ? "0" : progress);
        res.put("total", total == null ? "0" : total);

        return res;
    }
}