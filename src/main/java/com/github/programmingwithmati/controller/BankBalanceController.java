package com.github.programmingwithmati.controller;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.service.BankBalanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/bank-balance")
public class BankBalanceController {

    private final BankBalanceService bankBalanceService;

    @Autowired
    public BankBalanceController(BankBalanceService bankBalanceService) {
        this.bankBalanceService = bankBalanceService;
    }

    @GetMapping(value = "/{bankBalanceId}", produces = "application/json")
    public ResponseEntity<BankBalance> getBankBalance(@PathVariable("bankBalanceId") Long bankBalanceId) {
        var bankBalance = bankBalanceService.getBankBalance(bankBalanceId);
        return ResponseEntity.ok(bankBalance);
    }
}
