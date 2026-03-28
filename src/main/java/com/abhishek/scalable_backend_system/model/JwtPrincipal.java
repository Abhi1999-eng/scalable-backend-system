package com.abhishek.scalable_backend_system.model;

public class JwtPrincipal {

    private final Long userId;
    private final String email;
    private final String name;

    public JwtPrincipal(Long userId, String email, String name) {
        this.userId = userId;
        this.email = email;
        this.name = name;
    }

    public Long getUserId() {
        return userId;
    }

    public String getEmail() {
        return email;
    }

    public String getName() {
        return name;
    }
}
