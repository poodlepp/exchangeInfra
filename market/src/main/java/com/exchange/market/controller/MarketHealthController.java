package com.exchange.market.controller;

import com.exchange.common.api.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "market", description = "Market module health")
@RestController
@RequestMapping("/market")
public class MarketHealthController {

    @Operation(summary = "Market module health check")
    @GetMapping("/health")
    public R<String> health() {
        return R.ok("market ok");
    }
}
