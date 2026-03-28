package com.abhishek.scalable_backend_system.config;

import com.abhishek.scalable_backend_system.model.JwtPrincipal;
import com.abhishek.scalable_backend_system.service.JwtService;
import com.abhishek.scalable_backend_system.service.RequestAuthService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Map;

@Component
public class AuthInterceptor implements HandlerInterceptor {

    private final JwtService jwtService;
    private final ObjectMapper objectMapper;

    public AuthInterceptor(JwtService jwtService, ObjectMapper objectMapper) {
        this.jwtService = jwtService;
        this.objectMapper = objectMapper;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String authorization = request.getHeader("Authorization");
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            writeUnauthorized(response, "Missing bearer token.");
            return false;
        }

        try {
            JwtPrincipal principal = jwtService.verify(authorization.substring("Bearer ".length()));
            request.setAttribute(RequestAuthService.PRINCIPAL_REQUEST_ATTRIBUTE, principal);
            return true;
        } catch (IllegalArgumentException exception) {
            writeUnauthorized(response, exception.getMessage());
            return false;
        }
    }

    private void writeUnauthorized(HttpServletResponse response, String message) throws Exception {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getWriter(), Map.of("message", message));
    }
}
