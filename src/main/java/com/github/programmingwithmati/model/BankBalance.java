package com.github.programmingwithmati.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BankBalance {

    private Long id;
    private BigDecimal amount = BigDecimal.ZERO;
    private Date lastUpdate;
    private BankTransaction latestTransaction;

    public BankBalance process(BankTransaction bankTransaction) {
        this.id = bankTransaction.getBalanceId();
        this.latestTransaction = bankTransaction;
        if(this.amount.add(bankTransaction.getAmount()).compareTo(BigDecimal.ZERO) >= 0) {
            this.latestTransaction.setState(BankTransaction.BankTransactionState.APPROVED);
            this.amount = this.amount.add(bankTransaction.getAmount());
        } else {
            this.latestTransaction.setState(BankTransaction.BankTransactionState.REJECTED);
        }
        this.lastUpdate = bankTransaction.getTime();
        return this;
    }
}
