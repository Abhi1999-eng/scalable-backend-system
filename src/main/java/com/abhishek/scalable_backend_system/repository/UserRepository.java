package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User, Long> {
}