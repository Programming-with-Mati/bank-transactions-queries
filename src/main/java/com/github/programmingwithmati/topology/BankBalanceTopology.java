package com.github.programmingwithmati.topology;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.model.BankTransaction;
import com.github.programmingwithmati.model.JsonSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

public class BankBalanceTopology {

    public static final String BANK_TRANSACTIONS = "bank-transactions";
    public static final String BANK_BALANCES = "bank-balances";
    public static final String REJECTED_TRANSACTIONS = "rejected-transactions";

    public static Topology buildTopology() {
        Serde<BankTransaction> bankTransactionSerdes = new JsonSerde<>(BankTransaction.class);
        Serde<BankBalance> bankBalanceSerde = new JsonSerde<>(BankBalance.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<Long, BankBalance> bankBalancesStream = streamsBuilder.stream(BANK_TRANSACTIONS,
                Consumed.with(Serdes.Long(), bankTransactionSerdes))
                .groupByKey()
                .aggregate(BankBalance::new,
                        (key, value, aggregate) -> aggregate.process(value),
                        Materialized.with(Serdes.Long(), bankBalanceSerde))
                .toStream();
        bankBalancesStream
                .to(BANK_BALANCES, Produced.with(Serdes.Long(), bankBalanceSerde));

        bankBalancesStream
                .mapValues((readOnlyKey, value) -> value.getLatestTransaction())
                .filter((key, value) -> value.state == BankTransaction.BankTransactionState.REJECTED)
                .to(REJECTED_TRANSACTIONS, Produced.with(Serdes.Long(), bankTransactionSerdes));

        return streamsBuilder.build();
    }
}
