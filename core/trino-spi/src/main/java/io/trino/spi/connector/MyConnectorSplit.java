package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;

import java.util.List;

public class MyConnectorSplit implements ConnectorSplit {

    private final HostAddress address;

    @JsonCreator
    public MyConnectorSplit(
            @JsonProperty("address") HostAddress address)
    {
        this.address = address;
    }

    @JsonProperty
    public HostAddress getAddress()
    {
        return address;
    }

    @Override
    public boolean isRemotelyAccessible() {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return List.of(address);
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
