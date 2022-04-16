package com.github.programmingwithmati.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PossibleFraudAlert {

    private Long balanceId;
    private Long rejectedTransactionsCount;
    private String message;

}
