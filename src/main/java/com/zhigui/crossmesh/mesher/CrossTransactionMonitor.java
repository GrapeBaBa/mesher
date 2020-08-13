package com.zhigui.crossmesh.mesher;

import com.zhigui.crossmesh.mesher.resource.Resource;
import com.zhigui.crossmesh.mesher.resource.ResourceRegistry;
import org.springframework.beans.factory.annotation.Autowired;
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
import static com.zhigui.crossmesh.proto.Types.GlobalTransactionStatusType;

@Component
public class CrossTransactionMonitor {

    private final ResourceRegistry resourceRegistry;

    private final ScheduledExecutorService executorService;

    private final ExecutorService bookKeeper;

    private final CompletedGlobalTransactionCleaner completedGlobalTransactionCleaner;

    public CrossTransactionMonitor(Config config, ResourceRegistry resourceRegistry) {
        System.out.println(config.getCrossMonitorThreadNum());
        this.executorService = Executors.newScheduledThreadPool(config.getCrossMonitorThreadNum());
        this.bookKeeper = Executors.newSingleThreadExecutor();
        this.completedGlobalTransactionCleaner = new CompletedGlobalTransactionCleaner();
        this.resourceRegistry = resourceRegistry;
    }

    public void start() {
        this.bookKeeper.submit(this.completedGlobalTransactionCleaner);
    }

    public ScheduledFuture<?> monitor(BranchTransactionPreparedEvent preparedEvent) {
        return this.executorService.scheduleWithFixedDelay(() -> {
            Resource resource = resourceRegistry.getResource(preparedEvent.getPrimaryPrepareTxId().getUri());
            resource
                .evaluateGlobalTransaction(preparedEvent.getPrimaryPrepareTxId().getId())
                .whenComplete((globalTransactionStatus, throwable) -> {
                    if (throwable != null) {
                        throwable.printStackTrace();
                    }
                    BranchTransaction branchTransaction = null;
                    if (globalTransactionStatus.getStatus() == GlobalTransactionStatusType.PRIMARY_TRANSACTION_COMMITTED) {
                        branchTransaction = preparedEvent.getCommitTx();
                    } else if (globalTransactionStatus.getStatus() == GlobalTransactionStatusType.PRIMARY_TRANSACTION_CANCELED) {
                        branchTransaction = preparedEvent.getRollbackTx();
                    }
                    if (branchTransaction != null) {
                        resource.submitBranchTransaction(branchTransaction).whenComplete((branchTransactionResponse, throwable1) -> {
                            if (throwable1 != null) {
                                throwable1.printStackTrace();
                            }
                            this.completedGlobalTransactionCleaner.getCompletedGlobalTransactions().offer(preparedEvent);
                        });
                    }
                });

        }, 2, 2, TimeUnit.SECONDS);
    }

    public ConcurrentHashMap<BranchTransactionPreparedEvent, ScheduledFuture<?>> getPreparedBranchTransactions() {
        return this.completedGlobalTransactionCleaner.preparedBranchTransactions;
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
                    ScheduledFuture<?> preparedBranchTransactionFuture = preparedBranchTransactions.get(branchTransactionPreparedEvent);
                    preparedBranchTransactionFuture.cancel(true);
                }
            }
        }

    }
}
