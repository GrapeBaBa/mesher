package com.zhigui.crossmesh.mesher;

import com.zhigui.crossmesh.mesher.resource.Resource;
import com.zhigui.crossmesh.mesher.resource.ResourceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;

@Component
public class CrossTransactionMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrossTransactionMonitor.class);

    private final ResourceRegistry resourceRegistry;

    private final ScheduledExecutorService executorService;

    private final ExecutorService bookKeeper;

    private final CompletedGlobalTransactionCleaner completedGlobalTransactionCleaner;

    public CrossTransactionMonitor(Config config, ResourceRegistry resourceRegistry) {
        this.executorService = Executors.newScheduledThreadPool(config.getCrossMonitorThreadNum());
        this.bookKeeper = Executors.newSingleThreadExecutor();
        this.completedGlobalTransactionCleaner = new CompletedGlobalTransactionCleaner();
        this.resourceRegistry = resourceRegistry;
    }

    public void start() {
        this.bookKeeper.submit(this.completedGlobalTransactionCleaner);
    }

    public void stop() {
        this.bookKeeper.shutdown();
        this.executorService.shutdown();
    }

    public ScheduledFuture<?> monitor(BranchTransactionPreparedEvent preparedEvent) {
        return this.executorService.scheduleWithFixedDelay(() -> {
            Resource resource = resourceRegistry.getResource(preparedEvent.getPrimaryPrepareTxId().getUri());
            resource
                .evaluateGlobalTransaction(preparedEvent.getGlobalTxStatusQuery())
                .whenComplete((globalTransactionStatus, throwable) -> {
                    if (throwable != null) {
                        LOGGER.error("evaluate global transaction failed", throwable);
                        return;
                    }
                    resource.getProofForTransaction(globalTransactionStatus.getPrimaryConfirmTxId().getId()).whenComplete((proof, throwable1) -> {
                        Resource branchTxResource = resourceRegistry.getResource(preparedEvent.getConfirmTx().getTxId().getUri());
                        BranchTransaction.Builder builder=preparedEvent.toBuilder().getConfirmTxBuilder();
                        builder.getInvocationBuilder()
                        .addArgs(String.valueOf(globalTransactionStatus.getStatus().getNumber()))
                        .addArgs(globalTransactionStatus.getPrimaryConfirmTxId().getUri().getNetwork())
                        .addArgs(globalTransactionStatus.getPrimaryConfirmTxId().getUri().getChain())
                        .addArgs(globalTransactionStatus.getPrimaryConfirmTxId().getId())
                        .addArgs(proof);
                        BranchTransaction confirmTx=builder.build();
                        branchTxResource.submitBranchTransaction(confirmTx).whenComplete((branchTransactionResponse, e) -> {
                            if (e != null) {
                                LOGGER.error("submit branch tx failed", e);
                            }
                            completedGlobalTransactionCleaner.getCompletedGlobalTransactions().offer(preparedEvent);
                        });
                    });

                });
        }, 2, 2, TimeUnit.SECONDS);
    }

    public ConcurrentHashMap<BranchTransactionPreparedEvent, ScheduledFuture<?>> getPreparedBranchTransactions() {
        return this.completedGlobalTransactionCleaner.getPreparedBranchTransactions();
    }

    private static class CompletedGlobalTransactionCleaner implements Runnable {

        private final ConcurrentLinkedQueue<BranchTransactionPreparedEvent> completedGlobalTransactions;

        public ConcurrentLinkedQueue<BranchTransactionPreparedEvent> getCompletedGlobalTransactions() {
            return completedGlobalTransactions;
        }

        public ConcurrentHashMap<BranchTransactionPreparedEvent, ScheduledFuture<?>> getPreparedBranchTransactions() {
            return preparedBranchTransactions;
        }

        private final ConcurrentHashMap<BranchTransactionPreparedEvent, ScheduledFuture<?>> preparedBranchTransactions;

        public CompletedGlobalTransactionCleaner() {
            this.preparedBranchTransactions = new ConcurrentHashMap<>();
            this.completedGlobalTransactions = new ConcurrentLinkedQueue<>();
        }

        @Override
        public void run() {
            while (!Thread.interrupted()) {
                BranchTransactionPreparedEvent branchTransactionPreparedEvent = completedGlobalTransactions.poll();
                if (branchTransactionPreparedEvent != null) {
                    ScheduledFuture<?> preparedBranchTransactionFuture = preparedBranchTransactions.remove(branchTransactionPreparedEvent);
                    preparedBranchTransactionFuture.cancel(true);
                }
            }
        }

    }
}
