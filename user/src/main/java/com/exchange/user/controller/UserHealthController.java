package com.exchange.user.controller;

import com.exchange.common.api.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "user", description = "User module health")
@RestController
@RequestMapping("/user")
public class UserHealthController {

    @Operation(summary = "User module health check")
    @GetMapping("/health")
    public R<String> health() {
        return R.ok("user ok");
    }
}
