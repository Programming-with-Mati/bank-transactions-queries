package com.github.programmingwithmati.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.model.JsonSerde;
import com.github.programmingwithmati.topology.BankBalanceTopology;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Objects;

@Slf4j
@Component
public class BankBalanceRepository extends GenericKafkaStreamsRepository<Long, BankBalance> {

    public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public BankBalanceRepository(
            HostInfo hostInfo,
            KafkaStreams kafkaStreams
    ) {
        super(
                Serdes.Long(),
                new JsonSerde<>(BankBalance.class),
                hostInfo,
                kafkaStreams,
                BankBalanceTopology.BANK_BALANCES_STORE,
                "/bank-balance/%s");
    }

    @Override
    protected BankBalance findRemotely(Long key, HostInfo hostInfo) {
        log.info("Finding Bank Balance with key {} remotely in host {}", key, hostInfo);
        var url = "http://%s:%d" + findRemotelyUri;
        var urlWithParams = url.formatted(hostInfo.host(), hostInfo.port(), key.toString());
        var okHttpClient = new OkHttpClient();
        Request request = new Request.Builder().url(urlWithParams).build();
        try (Response response = okHttpClient.newCall(request).execute()) {
            return OBJECT_MAPPER.readValue(Objects.requireNonNull(response.body()).string(), BankBalance.class);
        } catch (Exception e) {
            throw new RuntimeException("Exception reading bank balance from remote server");
        }
//        return restTemplate.getForObject(urlWithParams, BankBalance.class);
    }
}
