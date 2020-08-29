package com.zhigui.crossmesh.mesher.resource;

import com.zhigui.crossmesh.proto.Types;

import java.util.concurrent.CompletableFuture;

import static com.zhigui.crossmesh.proto.Types.*;
import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse;
import static com.zhigui.crossmesh.proto.Types.GlobalTransactionStatus;

/**
 * Created by IntelliJ IDEA.
 * Author: kaichen
 * Date: 2020/7/17
 * Time: 10:19 AM
 */
public interface Resource {

    CompletableFuture<BranchTransactionResponse> submitBranchTransaction(BranchTransaction branchTx);

    CompletableFuture<GlobalTransactionStatus> evaluateGlobalTransaction(Invocation globalTxQuery);

    CompletableFuture<String> getProofForTransaction(String txId);
}