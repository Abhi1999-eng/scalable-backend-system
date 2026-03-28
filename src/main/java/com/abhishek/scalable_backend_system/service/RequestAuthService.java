package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.JwtPrincipal;
import com.abhishek.scalable_backend_system.model.User;
import com.abhishek.scalable_backend_system.repository.UserRepository;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Service;

@Service
public class RequestAuthService {

    public static final String PRINCIPAL_REQUEST_ATTRIBUTE = "jwtPrincipal";

    private final UserRepository userRepository;

    public RequestAuthService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public Long requireCurrentUserId(HttpServletRequest request) {
        return requirePrincipal(request).getUserId();
    }

    public User requireCurrentUser(HttpServletRequest request) {
        Long userId = requireCurrentUserId(request);
        return userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("Authenticated user was not found."));
    }

    private JwtPrincipal requirePrincipal(HttpServletRequest request) {
        Object attribute = request.getAttribute(PRINCIPAL_REQUEST_ATTRIBUTE);
        if (attribute instanceof JwtPrincipal principal) {
            return principal;
        }
        throw new IllegalArgumentException("Authentication is required.");
    }
}
