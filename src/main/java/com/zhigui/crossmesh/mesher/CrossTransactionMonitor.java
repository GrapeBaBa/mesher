package com.zhigui.crossmesh.mesher;

import com.zhigui.crossmesh.mesher.resource.Resource;
import com.zhigui.crossmesh.mesher.resource.ResourceRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionPreparedEvent;
import static java.lang.String.valueOf;

@Component
public class CrossTransactionMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CrossTransactionMonitor.class);

    private final ResourceRegistry resourceRegistry;

    private final ScheduledExecutorService executorService;

    private final ConcurrentHashMap<String, ScheduledFuture<?>> transactionFutures;

    public CrossTransactionMonitor(Config config, ResourceRegistry resourceRegistry) {
        this.executorService = Executors.newScheduledThreadPool(config.getCrossMonitorThreadNum());
        this.resourceRegistry = resourceRegistry;
        transactionFutures = new ConcurrentHashMap<>();
    }

    public void stop() {
        this.executorService.shutdown();
    }

    public void monitor(BranchTransactionPreparedEvent preparedEvent) {
        this.transactionFutures.computeIfAbsent(preparedEvent.getPrimaryPrepareTxId().getId(), s -> executorService.scheduleWithFixedDelay(() -> {
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
                        BranchTransaction.Builder builder = preparedEvent.toBuilder().getConfirmTxBuilder();
                        builder.getInvocationBuilder()
                            .addArgs(valueOf(globalTransactionStatus.getStatus().getNumber()))
                            .addArgs(globalTransactionStatus.getPrimaryConfirmTxId().getUri().getNetwork())
                            .addArgs(globalTransactionStatus.getPrimaryConfirmTxId().getUri().getChain())
                            .addArgs(globalTransactionStatus.getPrimaryConfirmTxId().getId())
                            .addArgs(proof);
                        BranchTransaction confirmTx = builder.build();
                        branchTxResource.submitBranchTransaction(confirmTx).whenComplete((branchTransactionResponse, e) -> {
                            if (e != null) {
                                LOGGER.error("submit branch tx failed", e);
                            }
                            transactionFutures.remove(preparedEvent.getPrimaryPrepareTxId().getId()).cancel(true);
                        });
                    });

                });
        }, 2, 1, TimeUnit.SECONDS));

    }

}
