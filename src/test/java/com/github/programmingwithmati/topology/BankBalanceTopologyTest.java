package com.github.programmingwithmati.topology;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.model.BankTransaction;
import com.github.programmingwithmati.model.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BankBalanceTopologyTest {

    TopologyTestDriver testDriver;
    private TestInputTopic<Long, BankTransaction> bankTransactionTopic;
    private TestOutputTopic<Long, BankBalance> bankBalanceTopic;
    private TestOutputTopic<Long, BankTransaction> rejectedBankTransactionTopic;

    @BeforeEach
    void setup() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        testDriver = new TopologyTestDriver(BankBalanceTopology.buildTopology(), props);

        var bankBalanceJsonSerde = new JsonSerde<>(BankBalance.class);
        var bankTransactionJsonSerde = new JsonSerde<>(BankTransaction.class);

        bankTransactionTopic = testDriver.createInputTopic(BankBalanceTopology.BANK_TRANSACTIONS, Serdes.Long().serializer(), bankTransactionJsonSerde.serializer());

        bankBalanceTopic = testDriver.createOutputTopic(BankBalanceTopology.BANK_BALANCES, Serdes.Long().deserializer(), bankBalanceJsonSerde.deserializer());
        rejectedBankTransactionTopic = testDriver.createOutputTopic(BankBalanceTopology.REJECTED_TRANSACTIONS, Serdes.Long().deserializer(), bankTransactionJsonSerde.deserializer());
    }

    @AfterEach
    void teardown() {
        testDriver.close();
    }

    @Test
    void testTopology() {
        List.of(
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(500))
                        .build(),
                BankTransaction.builder()
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(3000)).build(),
                BankTransaction.builder()
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(500)).build()
        )
                .forEach(bankTransaction -> bankTransactionTopic.pipeInput(bankTransaction.getBalanceId(), bankTransaction));

        var firstBalance = bankBalanceTopic.readValue();

        assertEquals(1L, firstBalance.getId());
        assertEquals(new BigDecimal(500), firstBalance.getAmount());

        var secondBalance = bankBalanceTopic.readValue();

        assertEquals(2L, secondBalance.getId());
        assertEquals(new BigDecimal(3000), secondBalance.getAmount());


        var thirdBalance = bankBalanceTopic.readValue();

        assertEquals(1L, thirdBalance.getId());
        assertEquals(new BigDecimal(1000), thirdBalance.getAmount());

        assertTrue(rejectedBankTransactionTopic.isEmpty());
    }

    @Test
    void testTopologyWhenRejection() {
        List.of(
                BankTransaction.builder()
                        .id(1L)
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(-500))
                        .build(),
                BankTransaction.builder()
                        .id(2L)
                        .balanceId(2L)
                        .time(new Date())
                        .amount(new BigDecimal(3000)).build(),
                BankTransaction.builder()
                        .id(3L)
                        .balanceId(1L)
                        .time(new Date())
                        .amount(new BigDecimal(500)).build()
        )
                .forEach(bankTransaction -> bankTransactionTopic.pipeInput(bankTransaction.getBalanceId(), bankTransaction));

        var firstBalance = bankBalanceTopic.readValue();

        assertEquals(1L, firstBalance.getId());
        assertEquals(new BigDecimal(0), firstBalance.getAmount());

        var secondBalance = bankBalanceTopic.readValue();

        assertEquals(2L, secondBalance.getId());
        assertEquals(new BigDecimal(3000), secondBalance.getAmount());


        var thirdBalance = bankBalanceTopic.readValue();

        assertEquals(1L, thirdBalance.getId());
        assertEquals(new BigDecimal(500), thirdBalance.getAmount());

        var bankTransaction = rejectedBankTransactionTopic.readValue();

        assertEquals(1L, bankTransaction.getId());
        assertEquals(BankTransaction.BankTransactionState.REJECTED, bankTransaction.getState());
    }
}
