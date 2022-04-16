package com.github.programmingwithmati.topology;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.model.BankTransaction;
import com.github.programmingwithmati.model.JsonSerde;
import com.github.programmingwithmati.model.PossibleFraudAlert;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Slf4j
public class BankBalanceTopology {

    public static final String BANK_TRANSACTIONS = "bank-transactions";
    public static final String BANK_BALANCES = "bank-balances";
    public static final String REJECTED_TRANSACTIONS = "rejected-transactions";
    public static final String BANK_BALANCES_STORE = "bank-balances-store";
    private static final Long FRAUD_ALERT_THRESHOLD = 10L;

    public static Topology buildTopology() {
        Serde<BankTransaction> bankTransactionSerde = new JsonSerde<>(BankTransaction.class);
        Serde<BankBalance> bankBalanceSerde = new JsonSerde<>(BankBalance.class);
        Serde<PossibleFraudAlert> possibleFraudAlertSerde = new JsonSerde<>(PossibleFraudAlert.class);
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<Long, BankBalance> bankBalancesStream = streamsBuilder.stream(BANK_TRANSACTIONS,
                Consumed.with(Serdes.Long(), bankTransactionSerde)
                        .withTimestampExtractor(new TransactionTimeExtractor()))
                .groupByKey()
                .aggregate(BankBalance::new,
                        (key, value, aggregate) -> aggregate.process(value),
                        Materialized.<Long, BankBalance, KeyValueStore<Bytes, byte[]>>as(BANK_BALANCES_STORE)
                            .withKeySerde(Serdes.Long())
                            .withValueSerde(bankBalanceSerde)
                )
                .toStream();
        bankBalancesStream
                .to(BANK_BALANCES, Produced.with(Serdes.Long(), bankBalanceSerde));

        var rejectedTransactionsStream = bankBalancesStream
                .mapValues((readOnlyKey, value) -> value.getLatestTransactions().first())
                .filter((key, value) -> value.state == BankTransaction.BankTransactionState.REJECTED);

        rejectedTransactionsStream
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.of(20L, ChronoUnit.SECONDS)).grace(Duration.ofSeconds(2L)))
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, value) -> log.info("Peek rejected transaction count for id: {}, value: {}", key.key(), value))
                .map((windowKey, value) -> KeyValue.pair(windowKey.key(), value))
                .filter((key, value) -> value >= FRAUD_ALERT_THRESHOLD)
                .mapValues((key, value) -> new PossibleFraudAlert(key, value, "Account %s had %d rejected transactions".formatted(key, value)))
                .to("possible-fraud-alert", Produced.with(Serdes.Long(), possibleFraudAlertSerde));

        rejectedTransactionsStream
                .to(REJECTED_TRANSACTIONS, Produced.with(Serdes.Long(), bankTransactionSerde));

        return streamsBuilder.build();
    }
}


