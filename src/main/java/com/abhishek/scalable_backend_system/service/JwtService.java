package com.abhishek.scalable_backend_system.service;

import com.abhishek.scalable_backend_system.model.JwtPrincipal;
import com.abhishek.scalable_backend_system.model.User;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class JwtService {

    private final ObjectMapper objectMapper;
    private final byte[] secretBytes;
    private final long expirationSeconds;

    public JwtService(
            ObjectMapper objectMapper,
            @Value("${app.auth.jwt-secret:change-me-super-secret}") String secret,
            @Value("${app.auth.jwt-expiration-seconds:86400}") long expirationSeconds) {
        this.objectMapper = objectMapper;
        this.secretBytes = secret.getBytes(StandardCharsets.UTF_8);
        this.expirationSeconds = expirationSeconds;
    }

    public String generateToken(User user) {
        Map<String, Object> header = Map.of("alg", "HS256", "typ", "JWT");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("sub", String.valueOf(user.getId()));
        payload.put("email", user.getEmail());
        payload.put("name", user.getName());
        payload.put("exp", Instant.now().plusSeconds(expirationSeconds).getEpochSecond());

        try {
            String headerEncoded = encode(objectMapper.writeValueAsBytes(header));
            String payloadEncoded = encode(objectMapper.writeValueAsBytes(payload));
            String signingInput = headerEncoded + "." + payloadEncoded;
            return signingInput + "." + sign(signingInput);
        } catch (Exception exception) {
            throw new IllegalStateException("Unable to generate JWT token.", exception);
        }
    }

    public JwtPrincipal verify(String token) {
        try {
            String[] parts = token.split("\\.");
            if (parts.length != 3) {
                throw new IllegalArgumentException("Invalid token format.");
            }

            String signingInput = parts[0] + "." + parts[1];
            if (!sign(signingInput).equals(parts[2])) {
                throw new IllegalArgumentException("Invalid token signature.");
            }

            Map<String, Object> payload = objectMapper.readValue(
                    Base64.getUrlDecoder().decode(parts[1]),
                    new TypeReference<>() {}
            );

            long expiration = Long.parseLong(payload.get("exp").toString());
            if (Instant.now().getEpochSecond() > expiration) {
                throw new IllegalArgumentException("Token has expired.");
            }

            return new JwtPrincipal(
                    Long.parseLong(payload.get("sub").toString()),
                    payload.get("email").toString(),
                    payload.get("name").toString()
            );
        } catch (IllegalArgumentException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid authentication token.", exception);
        }
    }

    private String sign(String value) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secretBytes, "HmacSHA256"));
        return encode(mac.doFinal(value.getBytes(StandardCharsets.UTF_8)));
    }

    private String encode(byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }
}
