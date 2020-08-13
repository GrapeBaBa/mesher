package com.zhigui.crossmesh.mesher.resource.fabric;

import com.google.protobuf.InvalidProtocolBufferException;
import com.zhigui.crossmesh.mesher.Coordinator;
import com.zhigui.crossmesh.mesher.Mesher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperledger.fabric.gateway.ContractEvent;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionConfirmedEvent;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionPreparedEvent;


public class CrossContractListener implements Consumer<ContractEvent> {

    private static final Log logger = LogFactory.getLog(CrossContractListener.class);
    public static final String PRIMARY_TRANSACTION_PREPARED_EVENT = "PRIMARY_TRANSACTION_PREPARED_EVENT";
    public static final String PRIMARY_TRANSACTION_CONFIRMED_EVENT = "PRIMARY_TRANSACTION_CONFIRMED_EVENT";
    public static final String BRANCH_TRANSACTION_PREPARED_EVENT = "BRANCH_TRANSACTION_PREPARED_EVENT";

    public static final String RESOURCE_REGISTERED_EVENT = "RESOURCE_REGISTERED_EVENT";

    private final String name;

    private final Coordinator coordinator;

    public CrossContractListener(String name, Coordinator coordinator) {
        this.name = name;
        this.coordinator = coordinator;
    }

    @Override
    public void accept(ContractEvent contractEvent) {
        String eventName = contractEvent.getName();
        Optional<byte[]> payloadOpt = contractEvent.getPayload();
        if (!payloadOpt.isPresent()) {
            return;
        }

        switch (eventName) {
            case PRIMARY_TRANSACTION_PREPARED_EVENT:
                if (!contractEvent.getTransactionEvent().isValid()) {
                    return;
                }
                PrimaryTransactionPreparedEvent primaryTransactionPreparedEvent;
                try {
                    primaryTransactionPreparedEvent = PrimaryTransactionPreparedEvent.parseFrom(payloadOpt.get());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    return;
                }
                coordinator.handlePrimaryTransactionPrepared(primaryTransactionPreparedEvent);
            case PRIMARY_TRANSACTION_CONFIRMED_EVENT:
                if (!contractEvent.getTransactionEvent().isValid()) {
                    return;
                }
                PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent;
                try {
                    primaryTransactionConfirmedEvent = PrimaryTransactionConfirmedEvent.parseFrom(payloadOpt.get());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    return;
                }
                coordinator.handlePrimaryTransactionConfirmed(primaryTransactionConfirmedEvent);
            case BRANCH_TRANSACTION_PREPARED_EVENT:
                BranchTransactionPreparedEvent branchTransactionPreparedEvent;
                try {
                    branchTransactionPreparedEvent = BranchTransactionPreparedEvent.parseFrom(payloadOpt.get());
                } catch (InvalidProtocolBufferException e) {
                    e.printStackTrace();
                    return;
                }
                coordinator.handleBranchTransactionPrepared(branchTransactionPreparedEvent);
//            case RESOURCE_REGISTERED_EVENT:
//                ResourceRegisteredEvent resourceRegisteredEvent;
//                try {
//
//                }
            default:
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CrossContractListener that = (CrossContractListener) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
