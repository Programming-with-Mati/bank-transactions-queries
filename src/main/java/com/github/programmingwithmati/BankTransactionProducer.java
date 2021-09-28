package com.github.programmingwithmati;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.model.BankTransaction;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class BankTransactionProducer {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        KafkaProducer<Long, String> bankTransactionProducer =
                new KafkaProducer<>(Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092",
                        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class,
                        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
                ));


        List<BankTransaction> data1 = List.of(
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .time(new Date())
                        .concept("Incomme")
                        .amount(new BigDecimal(4000))
                        .build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(3000)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Amazon")
                        .time(new Date())
                        .amount(new BigDecimal(-50)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Rent")
                        .time(new Date())
                        .amount(new BigDecimal(-1000)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Electricity")
                        .time(new Date())
                        .amount(new BigDecimal(-100)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Wallmart")
                        .time(new Date())
                        .amount(new BigDecimal(-60)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Vodafone")
                        .time(new Date())
                        .amount(new BigDecimal(-25)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Amazon")
                        .time(new Date())
                        .amount(new BigDecimal(-20)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Netflix")
                        .time(new Date())
                        .amount(new BigDecimal(-10)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Transport")
                        .time(new Date())
                        .amount(new BigDecimal(-10)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .concept("Transport")
                        .time(new Date())
                        .amount(new BigDecimal(-10)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(4L)
                        .time(new Date())
                        .amount(new BigDecimal(2000)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(4L)
                        .time(new Date())
                        .amount(new BigDecimal(-2500)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(3L)
                        .time(new Date())
                        .amount(new BigDecimal(1000)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(-500)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(-4000)).build(),
                BankTransaction.builder()
                        .id(UUID.randomUUID().toString())
                        .balanceId(3L)
                        .time(new Date())
                        .amount(new BigDecimal(-500)).build()
        );
        data1.stream()
                .map(bankTransaction -> new ProducerRecord<>("bank-transactions", bankTransaction.getBalanceId(), toJson(bankTransaction)))
                .forEach(record -> send(bankTransactionProducer, record));

        BankTransaction bankTransaction = BankTransaction.builder()
                .id(UUID.randomUUID().toString())
                .balanceId(3L)
                .time(new Date())
                .amount(new BigDecimal(-10_000)).build();

        send(bankTransactionProducer, new ProducerRecord<>("bank-transactions", bankTransaction.getBalanceId(), toJson(bankTransaction)));

    }

    @SneakyThrows
    private static void send(KafkaProducer<Long, String> bankTransactionProducer, ProducerRecord<Long, String> record) {
        bankTransactionProducer.send(record).get();
    }

    @SneakyThrows
    private static String toJson(BankTransaction bankTransaction) {
        return OBJECT_MAPPER.writeValueAsString(bankTransaction);
    }
}
