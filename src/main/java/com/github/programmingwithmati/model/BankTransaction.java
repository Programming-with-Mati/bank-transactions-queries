package com.github.programmingwithmati.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class BankTransaction implements Comparable<BankTransaction> {

    private String id;
    private Long balanceId;
    private String concept;
    private BigDecimal amount;
    @JsonFormat(shape = JsonFormat.Shape.STRING,
                pattern = "dd-MM-yyyy hh:mm:ss")
    public Date time;
    @Builder.Default
    public BankTransactionState state = BankTransactionState.CREATED;

    @Override
    public int compareTo(BankTransaction o) {
        var r = o.time.compareTo(this.time);
        if(r == 0) return o.id.compareTo(this.id);
        return r;
    }

    public static enum BankTransactionState {
        CREATED, APPROVED, REJECTED
    }
}
