package com.zhigui.crossmesh.mesher;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static com.zhigui.crossmesh.proto.Types.*;

/**
 * Created by IntelliJ IDEA.
 * Author: kaichen
 * Date: 2020/8/12
 * Time: 4:32 PM
 */
@ConfigurationProperties(prefix = "mesher")
public class Config {
    @Value("${mesher.self-network-name}")
    private String selfNetwork;

    @Value("${mesher.id-base-path}")
    private String idBasePath;

    @Value("${mesher.coordinator.cross-monitor-thread-num}")
    private int crossMonitorThreadNum;

    @Value("${mesher.meta-network-name}")
    private String metaNetwork;

    @Value("${mesher.meta-chain.name}")
    private String metaChain;

    @Value("${mesher.meta-chain.type}")
    private ChainType metaChainType;

    @Value("${mesher.meta-chain.fabric.conn-path}")
    private String fabricMetaChainConn;

    public String getSelfNetwork() {
        return selfNetwork;
    }

    public void setSelfNetwork(String selfNetwork) {
        this.selfNetwork = selfNetwork;
    }

    public String getIdBasePath() {
        return idBasePath;
    }

    public void setIdBasePath(String idBasePath) {
        this.idBasePath = idBasePath;
    }

    public int getCrossMonitorThreadNum() {
        return crossMonitorThreadNum;
    }

    public void setCrossMonitorThreadNum(int crossMonitorThreadNum) {
        this.crossMonitorThreadNum = crossMonitorThreadNum;
    }

    public String getMetaNetwork() {
        return metaNetwork;
    }

    public void setMetaNetwork(String metaNetwork) {
        this.metaNetwork = metaNetwork;
    }

    public String getMetaChain() {
        return metaChain;
    }

    public void setMetaChain(String metaChain) {
        this.metaChain = metaChain;
    }

    public ChainType getMetaChainType() {
        return metaChainType;
    }

    public void setMetaChainType(ChainType metaChainType) {
        this.metaChainType = metaChainType;
    }

    public String getFabricMetaChainConn() {
        return fabricMetaChainConn;
    }

    public void setFabricMetaChainConn(String fabricMetaChainConn) {
        this.fabricMetaChainConn = fabricMetaChainConn;
    }
}
