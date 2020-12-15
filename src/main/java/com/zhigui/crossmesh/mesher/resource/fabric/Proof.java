package com.zhigui.crossmesh.mesher.resource.fabric;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by IntelliJ IDEA.
 * Author: kaichen
 * Date: 2020/8/10
 * Time: 3:18 PM
 */
public class Proof implements Serializable {
    private String proposalResponsePayload;

    private Set<EndorserInfo> endorserInfos;

    public String getProposalResponsePayload() {
        return proposalResponsePayload;
    }

    public void setProposalResponsePayload(String proposalResponsePayload) {
        this.proposalResponsePayload = proposalResponsePayload;
    }

    public Set<EndorserInfo> getEndorserInfos() {
        return endorserInfos;
    }

    public void setEndorserInfos(Set<EndorserInfo> endorserInfos) {
        this.endorserInfos = endorserInfos;
    }

}
