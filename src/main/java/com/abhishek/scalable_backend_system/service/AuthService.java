package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.AuthLoginRequest;
import com.abhishek.scalable_backend_system.model.AuthResponse;
import com.abhishek.scalable_backend_system.model.AuthSignupRequest;
import com.abhishek.scalable_backend_system.model.AuthUserResponse;
import com.abhishek.scalable_backend_system.model.User;
import com.abhishek.scalable_backend_system.repository.UserRepository;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordService passwordService;
    private final JwtService jwtService;

    public AuthService(UserRepository userRepository, PasswordService passwordService, JwtService jwtService) {
        this.userRepository = userRepository;
        this.passwordService = passwordService;
        this.jwtService = jwtService;
    }

    public AuthResponse signup(AuthSignupRequest request) {
        userRepository.findByEmailIgnoreCase(request.getEmail().trim())
                .ifPresent(user -> {
                    throw new IllegalArgumentException("An account with this email already exists.");
                });

        User user = new User();
        user.setName(request.getName().trim());
        user.setEmail(request.getEmail().trim().toLowerCase());
        user.setPasswordHash(passwordService.hash(request.getPassword()));
        user.setCreatedAt(Instant.now());

        User saved = userRepository.save(user);
        return new AuthResponse(jwtService.generateToken(saved), new AuthUserResponse(saved));
    }

    public AuthResponse login(AuthLoginRequest request) {
        User user = userRepository.findByEmailIgnoreCase(request.getEmail().trim())
                .orElseThrow(() -> new IllegalArgumentException("Invalid email or password."));

        if (!passwordService.matches(request.getPassword(), user.getPasswordHash())) {
            throw new IllegalArgumentException("Invalid email or password.");
        }

        return new AuthResponse(jwtService.generateToken(user), new AuthUserResponse(user));
    }

    public AuthUserResponse me(User user) {
        return new AuthUserResponse(user);
    }
}
