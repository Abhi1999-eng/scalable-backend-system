package com.abhishek.scalable_backend_system.controller;

import com.abhishek.scalable_backend_system.model.AuthLoginRequest;
import com.abhishek.scalable_backend_system.model.AuthResponse;
import com.abhishek.scalable_backend_system.model.AuthSignupRequest;
import com.abhishek.scalable_backend_system.model.AuthUserResponse;
import com.abhishek.scalable_backend_system.service.AuthService;
import com.abhishek.scalable_backend_system.service.RequestAuthService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/auth")
public class AuthController {

    private final AuthService authService;
    private final RequestAuthService requestAuthService;

    public AuthController(AuthService authService, RequestAuthService requestAuthService) {
        this.authService = authService;
        this.requestAuthService = requestAuthService;
    }

    @PostMapping("/signup")
    public ResponseEntity<AuthResponse> signup(@Valid @RequestBody AuthSignupRequest request) {
        return ResponseEntity.ok(authService.signup(request));
    }

    @PostMapping("/login")
    public ResponseEntity<AuthResponse> login(@Valid @RequestBody AuthLoginRequest request) {
        return ResponseEntity.ok(authService.login(request));
    }

    @GetMapping("/me")
    public ResponseEntity<AuthUserResponse> me(HttpServletRequest request) {
        return ResponseEntity.ok(authService.me(requestAuthService.requireCurrentUser(request)));
    }
}
