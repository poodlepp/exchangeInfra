package com.exchange.risk.controller;

import com.exchange.common.api.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "risk", description = "Risk module health")
@RestController
@RequestMapping("/risk")
public class RiskHealthController {

    @Operation(summary = "Risk module health check")
    @GetMapping("/health")
    public R<String> health() {
        return R.ok("risk ok");
    }
}
