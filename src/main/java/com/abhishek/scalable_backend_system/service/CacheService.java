package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.User;
import com.abhishek.scalable_backend_system.repository.UserRepository;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Service
public class CacheService {

    private final StringRedisTemplate redisTemplate;
    private final UserRepository userRepository;
    private final KafkaProducerService kafkaProducerService;

    public CacheService(StringRedisTemplate redisTemplate,
                        UserRepository userRepository,
                        KafkaProducerService kafkaProducerService) {
        this.redisTemplate = redisTemplate;
        this.userRepository = userRepository;
        this.kafkaProducerService = kafkaProducerService;
    }

    public String getUser(Long id) {

        String key = "user:" + id;

        String cached = redisTemplate.opsForValue().get(key);

        if (cached != null) {
            return "CACHE HIT → " + cached + " (id=" + id + ")";
        }

        User user = userRepository.findById(id).orElse(null);

        if (user == null) {
            return "USER NOT FOUND";
        }

        redisTemplate.opsForValue()
                .set(key, user.getName(), Duration.ofMinutes(10));

        return "CACHE MISS → DB VALUE → " + user.getName();
    }

    public List<User> getAllUsers() {
        return userRepository.findAll();
    }

    public User saveUser(String name) {

        User user = new User(name);

        User saved = userRepository.save(user);

        kafkaProducerService.sendUserCreatedEvent(saved.getId());

        return saved;
    }

    public String deleteUser(Long id) {

        if (!userRepository.existsById(id)) {
            return "User not found";
        }

        userRepository.deleteById(id);

        kafkaProducerService.sendUserDeletedEvent(id);

        return "User deleted and event published";
    }
}