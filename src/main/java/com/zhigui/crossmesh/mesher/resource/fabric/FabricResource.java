package com.zhigui.crossmesh.mesher.resource.fabric;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.zhigui.crossmesh.mesher.Config;
import com.zhigui.crossmesh.mesher.Coordinator;
import com.zhigui.crossmesh.mesher.resource.Resource;
import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.ContractException;
import org.hyperledger.fabric.gateway.Gateway;
import org.hyperledger.fabric.gateway.GatewayRuntimeException;
import org.hyperledger.fabric.gateway.Network;
import org.hyperledger.fabric.gateway.Transaction;
import org.hyperledger.fabric.gateway.Wallet;
import org.hyperledger.fabric.gateway.Wallets;
import org.hyperledger.fabric.sdk.BlockEvent.TransactionEvent;
import org.hyperledger.fabric.sdk.BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.zhigui.crossmesh.proto.Types.BranchTransaction;
import static com.zhigui.crossmesh.proto.Types.BranchTransactionResponse;
import static com.zhigui.crossmesh.proto.Types.GlobalTransactionStatus;
import static com.zhigui.crossmesh.proto.Types.Invocation;
import static com.zhigui.crossmesh.proto.Types.TransactionID;
import static com.zhigui.crossmesh.proto.Types.URI;

public class FabricResource implements Resource {
    private static final Logger LOGGER = LoggerFactory.getLogger(FabricResource.class);

    private String selfNetwork;

    private String baseUrl;

    private URI uri;

    private Network network;

    private ConcurrentHashMap<String, CompletableFuture<TransactionEvent>> transactionResults;

    private Coordinator coordinator;

    public FabricResource(URI uri, byte[] connection, Path connPath, Coordinator coordinator, Config config) {
        this.coordinator = coordinator;
        this.selfNetwork = config.getSelfNetwork();
        this.baseUrl = config.getIdBasePath();
        this.uri = uri;
        this.transactionResults = new ConcurrentHashMap<>();
        setConnection(connection, connPath);
    }

    @Override
    public CompletableFuture<BranchTransactionResponse> submitBranchTransaction(BranchTransaction branchTx, Invocation globalTxQuery) {
        return CompletableFuture.supplyAsync(() -> {
            String contractName = branchTx.getInvocation().getContract();
            if (network == null) {
                throw new RuntimeException("network service not found");
            }

            Contract contract = network.getContract(contractName);
            if (contract == null) {
                throw new RuntimeException("contract service not found");
            }
            if (!branchTx.getUri().equals(this.uri)) {
                throw new RuntimeException("resource uri not match transaction uri");
            }
            if (selfNetwork.equals(uri.getNetwork())) {
                contract.addContractListener(new CrossContractListener("CROSS_CONTRACT_LISTENER", coordinator));
            }

            Transaction tx = contract.createTransaction(branchTx.getInvocation().getFunc());
            BranchTransactionResponse.Builder builder = BranchTransactionResponse.newBuilder();
            TransactionID branchTransactionId = TransactionID.newBuilder().setUri(this.uri).setId(tx.getTransactionId()).build();
            builder.setTxId(branchTransactionId);
            transactionResults.computeIfAbsent(tx.getTransactionId(), s -> new CompletableFuture<>());
            // Add global tx query invocation to branch transaction invocation args
            branchTx.getInvocation().getArgsList().add(globalTxQuery.toByteString().toString());
            try {
                tx.submit(branchTx.getInvocation().getArgsList().toArray(new String[0]));
                builder.setStatus(BranchTransactionResponse.Status.SUCCESS);
            } catch (ContractException | TimeoutException | InterruptedException | GatewayRuntimeException e) {
                LOGGER.error("submit transaction failed", e);
                builder.setStatus(BranchTransactionResponse.Status.FAILED);
            }

            try {
                String proofStr = getProofForTransaction(tx.getTransactionId()).get(30, TimeUnit.SECONDS);
                builder.setProof(ByteString.copyFromUtf8(proofStr));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("submit transaction failed", e);
                builder.setStatus(BranchTransactionResponse.Status.FAILED);
            }

            return builder.build();
        });
    }

    @Override
    public CompletableFuture<GlobalTransactionStatus> evaluateGlobalTransaction(Invocation globalTxQuery) {
        return null;
    }

    @Override
    public CompletableFuture<String> getProofForTransaction(String txId) {
        for (; ; ) {
            CompletableFuture<TransactionEvent> future = transactionResults.remove(txId);
            if (future != null) {
                return future.thenApply(transactionEvent -> {
                    TransactionActionInfo txInfo = transactionEvent.getTransactionActionInfo(0);
                    byte[] proposalResponsePayload = txInfo.getProposalResponsePayload();
                    Proof proof = new Proof();
                    proof.setProposalResponsePayload(proposalResponsePayload);
                    Set<EndorserInfo> endorserInfos = new HashSet<>();
                    for (int i = 0; i < txInfo.getEndorsementsCount(); i++) {
                        EndorserInfo endorserInfo = new EndorserInfo();
                        endorserInfo.setId(txInfo.getEndorsementInfo(i).getId());
                        endorserInfo.setMspId(txInfo.getEndorsementInfo(i).getMspid());
                        endorserInfo.setSignature(txInfo.getEndorsementInfo(i).getSignature());
                        endorserInfos.add(endorserInfo);
                    }
                    proof.setEndorserInfos(endorserInfos);
                    Gson proofJson = new Gson();
                    return proofJson.toJson(proof);
                });
            }
        }

    }

    private void setConnection(byte[] connection, Path connPath) {
        Path idDir = Paths.get(this.baseUrl, this.uri.getNetwork(), this.uri.getChain());
        Wallet wallet;
        try {
            wallet = Wallets.newFileSystemWallet(idDir);
        } catch (IOException e) {
            LOGGER.error("wallet path not found", e);
            throw new RuntimeException("identity dir not found");
        }

        Optional<String> identityOpt;
        try {
            identityOpt = wallet.list().stream().findFirst();
        } catch (IOException e) {
            LOGGER.error("wallet path list failed", e);
            throw new RuntimeException("identity dir list failed");
        }

        if (!identityOpt.isPresent()) {
            throw new RuntimeException("identity file not found");
        }

        Gateway.Builder builder;
        try {
            builder = Gateway.createBuilder()
                .identity(wallet, identityOpt.get());
            if (connPath != null) {
                builder.networkConfig(connPath);
            } else {
                builder.networkConfig(new ByteArrayInputStream(connection));
            }
        } catch (IOException e) {
            LOGGER.error("gateway build failed", e);
            throw new RuntimeException("gateway build failed");
        }

        Gateway gateway = builder.connect();
        Network network = gateway.getNetwork(this.uri.getChain());
        if (network == null) {
            throw new RuntimeException("network service create failed");
        }
        this.network = network;
        this.network.addBlockListener(blockEvent -> blockEvent.getTransactionEvents().forEach(transactionEvent -> {
            CompletableFuture<TransactionEvent> future = transactionResults.computeIfAbsent(transactionEvent.getTransactionID(), s -> new CompletableFuture<>());
            future.complete(transactionEvent);
        }));
        if (this.uri.getNetwork().equals(this.selfNetwork)) {
            Gson gson = new Gson();
            String connStr;
            if (connPath != null) {
                try {
                    connStr = new String(Files.readAllBytes(connPath));
                } catch (IOException e) {
                    LOGGER.error("read meta chain connection failed", e);
                    throw new RuntimeException("read meta chain connection failed");
                }
            } else {
                connStr = new String(connection);
            }
            JsonObject connJsonObj = gson.fromJson(connStr, JsonObject.class);
            connJsonObj.getAsJsonObject("channels").getAsJsonObject(this.uri.getChain()).getAsJsonArray("contracts").forEach(jsonElement -> {
                Contract contract = network.getContract(jsonElement.getAsString());
                if (contract == null) {
                    throw new RuntimeException("contract service not found");
                }
                contract.addContractListener(new CrossContractListener("CROSS_CONTRACT_LISTENER", coordinator));
            });
        }

    }
}
