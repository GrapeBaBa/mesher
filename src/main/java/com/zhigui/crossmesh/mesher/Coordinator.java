package com.zhigui.crossmesh.mesher;

import com.zhigui.crossmesh.mesher.resource.Resource;
import com.zhigui.crossmesh.mesher.resource.ResourceRegistry;
import com.zhigui.crossmesh.proto.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.zhigui.crossmesh.proto.Types.*;
import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransaction.*;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse.Status.SUCCESS;
import static com.zhigui.crossmesh.proto.Types.Invocation.*;
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

    public void handleResourceRegisteredEvent(ResourceRegisteredOrUpdatedEvent resourceRegisteredEvent) {
        this.resourceRegistry.handleResourceRegisteredEvent(resourceRegisteredEvent);
    }

    private void commitOrRollbackGlobalTransaction(PrimaryTransactionPreparedEvent primaryTxPreparedEvent, List<CompletableFuture<BranchTransactionResponse>> completableFutures) {
        BranchTransaction.Builder builder = primaryTxPreparedEvent.toBuilder().getPrimaryConfirmTxBuilder();
        Invocation.Builder invBuilder = builder.getInvocationBuilder();
        for (int i = 0; i < primaryTxPreparedEvent.getBranchPrepareTxsList().size(); i++) {
            invBuilder
                .addArgs(primaryTxPreparedEvent.getBranchPrepareTxs(i).getTxId().getUri().getNetwork())
                .addArgs(primaryTxPreparedEvent.getBranchPrepareTxs(i).getTxId().getUri().getChain());
            CompletableFuture<BranchTransactionResponse> branchTxResCompletableFuture = completableFutures.get(i);
            try {
                BranchTransactionResponse branchTxRes = branchTxResCompletableFuture.get(30, TimeUnit.SECONDS);
                invBuilder.addArgs(branchTxRes.getTxId().getId());
                if (branchTxRes.getStatus() == SUCCESS) {
                    invBuilder.addArgs(branchTxRes.getProof());
                } else {
                    invBuilder.addArgs("");
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("get branch transaction response failed", e);
                invBuilder.addArgs("");
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        BranchTransaction primaryConfirmTx = builder.build();
        Resource resource = resourceRegistry.getResource(primaryConfirmTx.getTxId().getUri());
        resource.submitBranchTransaction(primaryConfirmTx).whenComplete((branchTransactionResponse, throwable) -> {
            if (throwable != null) {
                LOGGER.error("submit primary commit tx failed", throwable);
            }
        });
    }

    private CompletableFuture<List<CompletableFuture<BranchTransactionResponse>>> prepareBranchTransaction(PrimaryTransactionPreparedEvent primaryTxPreparedEvent) {
        return resourceRegistry.getResource(primaryTxPreparedEvent.getPrimaryPrepareTxId().getUri())
            .getProofForTransaction(primaryTxPreparedEvent.getPrimaryPrepareTxId().getId())
            .thenApply(proof -> {
                LOGGER.info(proof);
                LOGGER.info("abc {}", primaryTxPreparedEvent);
                List<CompletableFuture<BranchTransactionResponse>> futureResList = new ArrayList<>();
                for (int i = 0; i < primaryTxPreparedEvent.getBranchPrepareTxsList().size(); i++) {
                    BranchTransaction.Builder branchPrepareTxBuilder = primaryTxPreparedEvent.toBuilder().getBranchPrepareTxsBuilder(i);
                    branchPrepareTxBuilder.getInvocationBuilder()
                        .addArgs(primaryTxPreparedEvent.getGlobalTxStatusQuery().getContract())
                        .addArgs(primaryTxPreparedEvent.getGlobalTxStatusQuery().getFunc())
                        .addArgs(primaryTxPreparedEvent.getPrimaryPrepareTxId().getUri().getNetwork())
                        .addArgs(primaryTxPreparedEvent.getPrimaryPrepareTxId().getUri().getChain())
                        .addArgs(primaryTxPreparedEvent.getPrimaryPrepareTxId().getId())
                        .addArgs(proof);
                    BranchTransaction branchPrepareTx = branchPrepareTxBuilder.build();
                    CompletableFuture<BranchTransactionResponse> response = resourceRegistry.getResource(branchPrepareTx.getTxId().getUri()).submitBranchTransaction(branchPrepareTx);
                    futureResList.add(response);
                }
                return futureResList;
            });
    }

    private void commitOrRollbackBranchTransaction(PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent) {
        resourceRegistry.getResource(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().getUri())
            .getProofForTransaction(primaryTransactionConfirmedEvent.getPrimaryConfirmTxId().getId())
            .thenAccept(proof -> primaryTransactionConfirmedEvent.toBuilder().getBranchConfirmTxsBuilderList()
                .parallelStream()
                .forEach(branchTransactionBuilder -> {
                    branchTransactionBuilder.getInvocationBuilder().addArgs(proof);
                    BranchTransaction branchTransaction = branchTransactionBuilder.build();
                    resourceRegistry.getResource(branchTransaction.getTxId().getUri()).submitBranchTransaction(branchTransaction);
                })).exceptionally(throwable -> {
            LOGGER.error("commit or rollback branch transaction error", throwable);
            return null;
        });

    }

    @PreDestroy
    public void stop() {
        this.crossTransactionMonitor.stop();
    }

    public ResourceRegistry getResourceRegistry() {
        return resourceRegistry;
    }
}
