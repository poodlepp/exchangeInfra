package com.exchange.wallet.controller;

import com.exchange.common.api.R;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "wallet", description = "Wallet module health")
@RestController
@RequestMapping("/wallet")
public class WalletHealthController {

    @Operation(summary = "Wallet module health check")
    @GetMapping("/health")
    public R<String> health() {
        return R.ok("wallet ok");
    }
}
