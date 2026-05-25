package com.exchange.common.security;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import lombok.extern.slf4j.Slf4j;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Map;

/**
 * Lightweight JWT helper backed by jjwt. Build a single instance via
 * {@link #of(String, Duration)} and reuse it.
 */
@Slf4j
public class JwtUtil {

    private final SecretKey signingKey;
    private final Duration ttl;

    private JwtUtil(SecretKey signingKey, Duration ttl) {
        this.signingKey = signingKey;
        this.ttl = ttl;
    }

    public static JwtUtil of(String secret, Duration ttl) {
        if (secret == null || secret.length() < 32) {
            throw new IllegalArgumentException("JWT secret must be at least 32 characters");
        }
        SecretKey key = Keys.hmacShaKeyFor(secret.getBytes(StandardCharsets.UTF_8));
        return new JwtUtil(key, ttl);
    }

    public String generate(String subject, Map<String, Object> claims) {
        Date now = new Date();
        Date expiresAt = new Date(now.getTime() + ttl.toMillis());
        return Jwts.builder()
                .subject(subject)
                .claims(claims)
                .issuedAt(now)
                .expiration(expiresAt)
                .signWith(signingKey)
                .compact();
    }

    public Claims parse(String token) {
        return Jwts.parser()
                .verifyWith(signingKey)
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    public boolean validate(String token) {
        try {
            parse(token);
            return true;
        } catch (JwtException | IllegalArgumentException e) {
            log.debug("JWT validation failed: {}", e.getMessage());
            return false;
        }
    }
}
