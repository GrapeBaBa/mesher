package com.zhigui.crossmesh.mesher;

import com.zhigui.crossmesh.mesher.resource.Resource;
import com.zhigui.crossmesh.mesher.resource.ResourceRegistry;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionConfirmedEvent;
import static com.zhigui.crossmesh.proto.Types.PrimaryTransactionPreparedEvent;

@Component
public class Coordinator {
    private final ResourceRegistry resourceRegistry;

    private final CrossTransactionMonitor crossTransactionMonitor;

    public Coordinator(CrossTransactionMonitor crossTransactionMonitor, ResourceRegistry resourceRegistry) {
        this.crossTransactionMonitor = crossTransactionMonitor;
        this.resourceRegistry = resourceRegistry;
    }

    public void start() {
        this.crossTransactionMonitor.start();
    }

    public void handleBranchTransactionPrepared(final BranchTransactionPreparedEvent branchTransactionPreparedEvent) {
        this.crossTransactionMonitor.getPreparedBranchTransactions().computeIfAbsent(branchTransactionPreparedEvent, this.crossTransactionMonitor::monitor);
    }

    public void handlePrimaryTransactionConfirmed(PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent) {
        commitOrRollbackBranchTransaction(primaryTransactionConfirmedEvent);
    }

    public void handlePrimaryTransactionPrepared(PrimaryTransactionPreparedEvent primaryTransactionPreparedEvent) {
        boolean prepareFailed = prepareBranchTransaction(primaryTransactionPreparedEvent);
        commitOrRollbackPrimaryTransaction(!prepareFailed, primaryTransactionPreparedEvent);
    }

    private void commitOrRollbackPrimaryTransaction(boolean isAllBranchTransactionPrepared, PrimaryTransactionPreparedEvent primaryTransactionPreparedEvent) {
        BranchTransaction branchTransaction;
        if (isAllBranchTransactionPrepared) {
            branchTransaction = primaryTransactionPreparedEvent.getPrimaryCommitTx();
        } else {
            branchTransaction = primaryTransactionPreparedEvent.getPrimaryRollbackTx();
        }

        Resource resource = resourceRegistry.getResource(branchTransaction.getTxId().getUri());
        resource.submitBranchTransaction(branchTransaction).whenComplete((branchTransactionResponse, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
            }
        });
        //TODO: If here is failed, currently primary prepared tx will be released if other tx read write some keys locked by this primary prepared tx.
    }

    // TODO: Add proof to branchTransaction
    private boolean prepareBranchTransaction(PrimaryTransactionPreparedEvent primaryTransactionPreparedEvent) {
        return primaryTransactionPreparedEvent.getBranchPrepareTxsList()
            .parallelStream()
            .map(branchTransaction -> resourceRegistry.getResource(branchTransaction.getTxId().getUri()).submitBranchTransaction(branchTransaction))
            .anyMatch(branchTransactionResponseCompletableFuture -> {
                try {
                    return branchTransactionResponseCompletableFuture.thenApply(branchTransactionResponse -> branchTransactionResponse.getStatus() == BranchTransactionResponse.Status.FAILED).get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    return true;
                }
            });
    }

    // TODO: Add proof to branchTransaction
    private void commitOrRollbackBranchTransaction(PrimaryTransactionConfirmedEvent primaryTransactionConfirmedEvent) {
        primaryTransactionConfirmedEvent.getBranchCommitOrRollbackTxsList()
            .parallelStream()
            .forEach(branchTransaction -> resourceRegistry.getResource(branchTransaction.getTxId().getUri()).submitBranchTransaction(branchTransaction));
    }

    public void stop() {

    }
}
