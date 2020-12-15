package com.zhigui.crossmesh.mesher.resource.fabric;

import com.google.protobuf.InvalidProtocolBufferException;
import com.zhigui.crossmesh.mesher.Coordinator;
import org.hyperledger.fabric.gateway.ContractEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionConfirmedEvent;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionPreparedEvent;
import static com.zhigui.crossmesh.proto.Types.ResourceRegisteredOrUpdatedEvent;


public class CrossContractListener implements Consumer<ContractEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CrossContractListener.class);
    public static final String PRIMARY_TRANSACTION_PREPARED_EVENT = "SIDE_MESH_PRIMARY_TRANSACTION_PREPARED_EVENT";
    public static final String PRIMARY_TRANSACTION_CONFIRMED_EVENT = "SIDE_MESH_PRIMARY_TRANSACTION_CONFIRMED_EVENT";
    public static final String BRANCH_TRANSACTION_PREPARED_EVENT = "SIDE_MESH_BRANCH_TRANSACTION_PREPARED_EVENT";
    public static final String RESOURCE_REGISTERED_EVENT = "SIDE_MESH_RESOURCE_REGISTERED_EVENT";

    private final String name;

    private final Coordinator coordinator;

    private final String contractName;

    public CrossContractListener(String name, Coordinator coordinator, String contractName) {
        this.name = name;
        this.coordinator = coordinator;
        this.contractName = contractName;
    }

    @Override
    public void accept(ContractEvent contractEvent) {
        String eventName = contractEvent.getName();
        LOGGER.info(eventName);
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
                    LOGGER.error("parse primary tx prepared event exception", e);
                    return;
                }
                PrimaryTransactionPreparedEvent.Builder primaryTransactionPreparedEventBuilder = primaryTransactionPreparedEvent.toBuilder();
                primaryTransactionPreparedEventBuilder.getPrimaryConfirmTxBuilder().getInvocationBuilder().setContract(this.contractName);
                primaryTransactionPreparedEventBuilder.getGlobalTxStatusQueryBuilder().setContract(this.contractName);
                primaryTransactionPreparedEvent = primaryTransactionPreparedEventBuilder.build();
                FabricResource fabricResourceForPrepare = (FabricResource) coordinator.getResourceRegistry().getResource(primaryTransactionPreparedEvent.getPrimaryPrepareTxId().getUri());
                fabricResourceForPrepare.addTransactionEvent(contractEvent.getTransactionEvent().getTransactionID(), contractEvent.getTransactionEvent());
                coordinator.handlePrimaryTransactionPrepared(primaryTransactionPreparedEventBuilder.build());
                break;
            case PRIMARY_TRANSACTION_CONFIRMED_EVENT:
                if (!contractEvent.getTransactionEvent().isValid()) {
                    return;
                }
                PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent;
                try {
                    primaryTransactionConfirmedEvent = PrimaryTransactionConfirmedEvent.parseFrom(payloadOpt.get());
                } catch (InvalidProtocolBufferException e) {
                    LOGGER.error("parse primary tx confirmed event exception", e);
                    return;
                }
                FabricResource fabricResourceForConfirm = (FabricResource) coordinator.getResourceRegistry().getResource(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().getUri());
                fabricResourceForConfirm.addTransactionEvent(contractEvent.getTransactionEvent().getTransactionID(), contractEvent.getTransactionEvent());
                coordinator.handlePrimaryTransactionConfirmed(primaryTransactionConfirmedEvent);
                break;
            case BRANCH_TRANSACTION_PREPARED_EVENT:
                BranchTransactionPreparedEvent branchTransactionPreparedEvent;
                try {
                    branchTransactionPreparedEvent = BranchTransactionPreparedEvent.parseFrom(payloadOpt.get());
                } catch (InvalidProtocolBufferException e) {
                    LOGGER.error("parse branch tx prepared event exception", e);
                    return;
                }
                BranchTransactionPreparedEvent.Builder branchTransactionPreparedEventBuilder = branchTransactionPreparedEvent.toBuilder();
                branchTransactionPreparedEventBuilder.getConfirmTxBuilder().getInvocationBuilder().setContract(this.contractName);
                coordinator.handleBranchTransactionPrepared(branchTransactionPreparedEventBuilder.build());
                break;
            case RESOURCE_REGISTERED_EVENT:
                ResourceRegisteredOrUpdatedEvent resourceRegisteredOrUpdatedEvent;
                try {
                    resourceRegisteredOrUpdatedEvent = ResourceRegisteredOrUpdatedEvent.parseFrom(payloadOpt.get());
                } catch (InvalidProtocolBufferException e) {
                    LOGGER.error("resource registered or update event exception", e);
                    return;
                }
                coordinator.handleResourceRegisteredEvent(resourceRegisteredOrUpdatedEvent);
                break;
            default:
                break;
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
