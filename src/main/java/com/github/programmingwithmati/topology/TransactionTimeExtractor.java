package com.github.programmingwithmati.topology;

import com.github.programmingwithmati.model.BankTransaction;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Optional;

public class TransactionTimeExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        var bankTransaction = (BankTransaction) record.value();
        return Optional.ofNullable(bankTransaction.getTime())
                .map(it -> it.toInstant().toEpochMilli())
                .orElse(partitionTime);
    }
}
