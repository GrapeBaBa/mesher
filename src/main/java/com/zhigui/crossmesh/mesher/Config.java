package com.zhigui.crossmesh.mesher;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;

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
}
