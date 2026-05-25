package com.exchange.trade.controller;

import com.exchange.common.api.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "trade", description = "Trade module health")
@RestController
@RequestMapping("/trade")
public class TradeHealthController {

    @Operation(summary = "Trade module health check")
    @GetMapping("/health")
    public R<String> health() {
        return R.ok("trade ok");
    }
}
