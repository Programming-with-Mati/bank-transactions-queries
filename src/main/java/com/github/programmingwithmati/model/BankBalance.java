package com.github.programmingwithmati.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;
import java.util.TreeSet;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BankBalance {

    private Long id;
    private BigDecimal amount = BigDecimal.ZERO;
    @JsonFormat(shape = JsonFormat.Shape.STRING,
            pattern = "dd-MM-yyyy hh:mm:ss")
    private Date lastUpdate;
    private TreeSet<BankTransaction> latestTransactions = new TreeSet<>();

    public BankBalance process(BankTransaction bankTransaction) {
        this.id = bankTransaction.getBalanceId();
        var transactionBuilder = bankTransaction.toBuilder();
        if(this.amount.add(bankTransaction.getAmount()).compareTo(BigDecimal.ZERO) >= 0) {
            addLatestTransaction(transactionBuilder.state(BankTransaction.BankTransactionState.APPROVED).build());
            this.amount = this.amount.add(bankTransaction.getAmount());
        } else {
            addLatestTransaction(transactionBuilder.state(BankTransaction.BankTransactionState.REJECTED).build());
        }
        this.lastUpdate = bankTransaction.getTime();
        return this;
    }

    private void addLatestTransaction(BankTransaction transactionClone) {
        if (latestTransactions.size() > 10) latestTransactions.pollLast();
        latestTransactions.add(transactionClone);
    }
}
