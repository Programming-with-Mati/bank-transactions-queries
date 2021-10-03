package com.github.programmingwithmati.service;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.repository.BankBalanceRepository;
import org.springframework.stereotype.Service;

@Service
public class BankBalanceService {

    private final BankBalanceRepository bankBalanceRepository;

    public BankBalanceService(BankBalanceRepository bankBalanceRepository) {
        this.bankBalanceRepository = bankBalanceRepository;
    }

    public BankBalance getBankBalance(Long bankBalanceId) {
        return bankBalanceRepository.find(bankBalanceId);
    }

}
