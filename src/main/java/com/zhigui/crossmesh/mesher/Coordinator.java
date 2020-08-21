package com.zhigui.crossmesh.mesher;

import com.google.protobuf.ProtocolStringList;
import com.zhigui.crossmesh.mesher.resource.Resource;
import com.zhigui.crossmesh.mesher.resource.ResourceRegistry;
import com.zhigui.crossmesh.proto.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionConfirmedEvent;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionPreparedEvent;

@Component
public class Coordinator {
    private static final Logger LOGGER = LoggerFactory.getLogger(Coordinator.class);

    @Autowired
    private ResourceRegistry resourceRegistry;

    @Autowired
    private CrossTransactionMonitor crossTransactionMonitor;

    @PostConstruct
    public void start() {
        this.crossTransactionMonitor.start();
    }

    public void handleBranchTransactionPrepared(final BranchTransactionPreparedEvent branchTransactionPreparedEvent) {
        this.crossTransactionMonitor.getPreparedBranchTransactions().computeIfAbsent(branchTransactionPreparedEvent, this.crossTransactionMonitor::monitor);
    }

    public void handlePrimaryTransactionConfirmed(final PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent) {
        commitOrRollbackBranchTransaction(primaryTransactionConfirmedEvent);
    }

    public void handlePrimaryTransactionPrepared(PrimaryTransactionPreparedEvent primaryTransactionPreparedEvent) {
        prepareBranchTransaction(primaryTransactionPreparedEvent).thenAccept(completableFutures -> commitOrRollbackGlobalTransaction(primaryTransactionPreparedEvent, completableFutures)).exceptionally(throwable -> {
            LOGGER.error("prepare branch transactions error", throwable);
            return null;
        });
    }

    public void handleResourceRegisteredEvent(Types.ResourceRegisteredOrUpdatedEvent resourceRegisteredEvent) {
        this.resourceRegistry.handleResourceRegisteredEvent(resourceRegisteredEvent);
    }

    private void commitOrRollbackGlobalTransaction(PrimaryTransactionPreparedEvent primaryTransactionPreparedEvent, List<CompletableFuture<BranchTransactionResponse>> completableFutures) {
        List<String> branchTxProofs = completableFutures.stream().map(branchTransactionResponseCompletableFuture -> {
            try {
                if (branchTransactionResponseCompletableFuture.get().getStatus() == BranchTransactionResponse.Status.FAILED) {
                    return null;
                }
                return branchTransactionResponseCompletableFuture.get().getProof().toString();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("get branch transaction response failed", e);
                return null;
            }
        }).collect(Collectors.toList());
        boolean prepareFailed = branchTxProofs.stream().anyMatch(Objects::isNull) || branchTxProofs.size() < primaryTransactionPreparedEvent.getBranchPrepareTxsList().size();
        BranchTransaction primaryConfirmTx;
        if (!prepareFailed) {
            primaryTransactionPreparedEvent.getPrimaryCommitTx().getInvocation().getArgsList().addAll(branchTxProofs);
            primaryConfirmTx = primaryTransactionPreparedEvent.getPrimaryCommitTx();
        } else {
            primaryConfirmTx = primaryTransactionPreparedEvent.getPrimaryRollbackTx();
        }

        Resource resource = resourceRegistry.getResource(primaryConfirmTx.getUri());
        resource.submitBranchTransaction(primaryTransactionPreparedEvent.getPrimaryCommitTx(), null).whenComplete((branchTransactionResponse, throwable) -> {
            if (throwable != null) {
                LOGGER.error("submit primary commit tx failed", throwable);
            }
        });
    }

    private CompletableFuture<List<CompletableFuture<BranchTransactionResponse>>> prepareBranchTransaction(PrimaryTransactionPreparedEvent primaryTxPreparedEvent) {
        return resourceRegistry.getResource(primaryTxPreparedEvent.getPrimaryPrepareTxId().getUri())
            .getProofForTransaction(primaryTxPreparedEvent.getPrimaryPrepareTxId().getId())
            .thenApply(proof -> primaryTxPreparedEvent.getBranchPrepareTxsList()
                .parallelStream()
                .map(branchTransaction -> {
                    // add primary prepare tx proof to branch transaction invocation arg
                    ProtocolStringList argsList = branchTransaction.getInvocation().getArgsList();
                    argsList.add(primaryTxPreparedEvent.getPrimaryPrepareTxId().toByteString().toString());
                    argsList.add(proof);
                    return resourceRegistry.getResource(branchTransaction.getUri()).submitBranchTransaction(branchTransaction, primaryTxPreparedEvent.getGlobalTxStatusQuery());
                }).collect(Collectors.toList()));
    }

    private void commitOrRollbackBranchTransaction(PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent) {
        resourceRegistry.getResource(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().getUri())
            .getProofForTransaction(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().getId())
            .thenAccept(proof -> primaryTransactionConfirmedEvent.getBranchCommitOrRollbackTxsList()
                .parallelStream()
                .forEach(branchTransaction -> {
                    ProtocolStringList argsList = branchTransaction.getInvocation().getArgsList();
                    argsList.add(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().toByteString().toString());
                    argsList.add(proof);
                    resourceRegistry.getResource(branchTransaction.getUri()).submitBranchTransaction(branchTransaction, null);
                })).exceptionally(throwable -> {
            LOGGER.error("commit or rollback branch transaction error", throwable);
            return null;
        });

    }

    public void stop() {

    }
}
