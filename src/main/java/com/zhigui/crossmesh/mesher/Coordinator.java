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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse.Status.SUCCESS;
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
        for (int i = 0; i < primaryTransactionPreparedEvent.getBranchPrepareTxsList().size(); i++) {
            primaryTransactionPreparedEvent.getPrimaryConfirmTx().getInvocation().getArgsList().add(primaryTransactionPreparedEvent.getBranchPrepareTxs(0).getTxId().getUri().getNetwork());
            primaryTransactionPreparedEvent.getPrimaryConfirmTx().getInvocation().getArgsList().add(primaryTransactionPreparedEvent.getBranchPrepareTxs(0).getTxId().getUri().getChain());
            primaryTransactionPreparedEvent.getPrimaryConfirmTx().getInvocation().getArgsList().add(primaryTransactionPreparedEvent.getBranchPrepareTxs(0).getTxId().getId());
            CompletableFuture<BranchTransactionResponse> branchTxResCompletableFuture = completableFutures.get(0);
            try {
                BranchTransactionResponse branchTxRes = branchTxResCompletableFuture.get(30, TimeUnit.SECONDS);
                if (branchTxRes.getStatus() == SUCCESS) {
                    primaryTransactionPreparedEvent.getPrimaryConfirmTx().getInvocation().getArgsList().add(branchTxRes.getProof().toString());
                } else {
                    primaryTransactionPreparedEvent.getPrimaryConfirmTx().getInvocation().getArgsList().add("");
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("get branch transaction response failed", e);
                primaryTransactionPreparedEvent.getPrimaryConfirmTx().getInvocation().getArgsList().add("");
            }
        }

        Resource resource = resourceRegistry.getResource(primaryTransactionPreparedEvent.getPrimaryConfirmTx().getTxId().getUri());
        resource.submitBranchTransaction(primaryTransactionPreparedEvent.getPrimaryConfirmTx()).whenComplete((branchTransactionResponse, throwable) -> {
            if (throwable != null) {
                LOGGER.error("submit primary commit tx failed", throwable);
            }
        });
    }

    private CompletableFuture<List<CompletableFuture<BranchTransactionResponse>>> prepareBranchTransaction(PrimaryTransactionPreparedEvent primaryTxPreparedEvent) {
        return resourceRegistry.getResource(primaryTxPreparedEvent.getPrimaryPrepareTxId().getUri())
            .getProofForTransaction(primaryTxPreparedEvent.getPrimaryPrepareTxId().getId())
            .thenApply(proof -> {
                List<CompletableFuture<BranchTransactionResponse>> futureResList = new ArrayList<>();
                for (int i = 0; i < primaryTxPreparedEvent.getBranchPrepareTxsList().size(); i++) {
                    BranchTransaction branchPrepareTx = primaryTxPreparedEvent.getBranchPrepareTxs(i);
                    ProtocolStringList argsList = branchPrepareTx.getInvocation().getArgsList();
                    argsList.add(primaryTxPreparedEvent.getGlobalTxStatusQuery().getContract());
                    argsList.add(primaryTxPreparedEvent.getGlobalTxStatusQuery().getFunc());
                    argsList.add(primaryTxPreparedEvent.getPrimaryPrepareTxId().getUri().getNetwork());
                    argsList.add(primaryTxPreparedEvent.getPrimaryPrepareTxId().getUri().getChain());
                    argsList.add(primaryTxPreparedEvent.getPrimaryPrepareTxId().getId());
                    argsList.add(proof);
                    CompletableFuture<BranchTransactionResponse> response = resourceRegistry.getResource(branchPrepareTx.getTxId().getUri()).submitBranchTransaction(branchPrepareTx);
                    futureResList.add(response);
                }
                return futureResList;
            });
    }

    private void commitOrRollbackBranchTransaction(PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent) {
        resourceRegistry.getResource(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().getUri())
            .getProofForTransaction(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().getId())
            .thenAccept(proof -> primaryTransactionConfirmedEvent.getBranchConfirmTxsList()
                .parallelStream()
                .forEach(branchTransaction -> {
                    ProtocolStringList argsList = branchTransaction.getInvocation().getArgsList();
                    argsList.add(proof);
                    resourceRegistry.getResource(branchTransaction.getTxId().getUri()).submitBranchTransaction(branchTransaction);
                })).exceptionally(throwable -> {
            LOGGER.error("commit or rollback branch transaction error", throwable);
            return null;
        });

    }

    public void stop() {

    }
}
