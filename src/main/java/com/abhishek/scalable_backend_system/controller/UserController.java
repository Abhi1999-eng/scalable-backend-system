package com.abhishek.scalable_backend_system.controller;

import com.abhishek.scalable_backend_system.model.User;
import com.abhishek.scalable_backend_system.service.CacheService;
import com.abhishek.scalable_backend_system.service.RateLimiterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

@RestController
@RequestMapping("/users")
public class UserController {

    private final CacheService cacheService;
    private final RateLimiterService rateLimiterService;

    public UserController(CacheService cacheService,
                          RateLimiterService rateLimiterService) {
        this.cacheService = cacheService;
        this.rateLimiterService = rateLimiterService;
    }

    /**
     * Create user
     */
    @PostMapping
    public ResponseEntity<?> createUser(@RequestParam String name,
                                        @RequestHeader(value = "client-id", defaultValue = "anonymous") String clientId) {

        boolean allowed = rateLimiterService.allowRequest(clientId);

        if (!allowed) {
            return ResponseEntity.status(429)
                    .body("Rate limit exceeded. Try again later.");
        }

        User user = cacheService.saveUser(name);

        return ResponseEntity.ok(user);
    }

    /**
     * Fetch user
     */
    @GetMapping("/{id}")
    public ResponseEntity<?> getUser(@PathVariable Long id,
                                     @RequestHeader(value = "client-id", defaultValue = "anonymous") String clientId) {

        boolean allowed = rateLimiterService.allowRequest(clientId);

        if (!allowed) {
            return ResponseEntity.status(429)
                    .body("Rate limit exceeded. Try again later.");
        }

        String result = cacheService.getUser(id);

        return ResponseEntity.ok(result);
    }

    /**
     * Delete user
     */
    @DeleteMapping("/{id}")
    public ResponseEntity<?> deleteUser(@PathVariable Long id,
                                        @RequestHeader(value = "client-id", defaultValue = "anonymous") String clientId) {

        boolean allowed = rateLimiterService.allowRequest(clientId);

        if (!allowed) {
            return ResponseEntity.status(429)
                    .body("Rate limit exceeded. Try again later.");
        }

        String result = cacheService.deleteUser(id);

        return ResponseEntity.ok(result);
    }

    @GetMapping
    public ResponseEntity<?> getAllUsers(
        @RequestHeader(value = "client-id", defaultValue = "anonymous") String clientId) {

        boolean allowed = rateLimiterService.allowRequest(clientId);

        if (!allowed) {
            return ResponseEntity.status(429)
                .body("Rate limit exceeded. Try again later.");
        }

        return ResponseEntity.ok(cacheService.getAllUsers());
    }
}