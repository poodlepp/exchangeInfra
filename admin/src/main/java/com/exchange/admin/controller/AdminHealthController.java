package com.exchange.admin.controller;

import com.exchange.common.api.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "admin", description = "Admin module health")
@RestController
@RequestMapping("/admin")
public class AdminHealthController {

    @Operation(summary = "Admin module health check")
    @GetMapping("/health")
    public R<String> health() {
        return R.ok("admin ok");
    }
}
