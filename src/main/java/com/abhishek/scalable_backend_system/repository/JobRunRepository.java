package com.abhishek.scalable_backend_system.repository;

import com.abhishek.scalable_backend_system.model.JobRun;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface JobRunRepository extends JpaRepository<JobRun, Long> {

    Optional<JobRun> findByJobId(Long jobId);
}
