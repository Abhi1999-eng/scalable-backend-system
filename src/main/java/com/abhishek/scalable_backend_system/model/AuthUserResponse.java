package com.abhishek.scalable_backend_system.model;

public class AuthUserResponse {

    private Long id;
    private String name;
    private String email;

    public AuthUserResponse() {
    }

    public AuthUserResponse(User user) {
        this.id = user.getId();
        this.name = user.getName();
        this.email = user.getEmail();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
