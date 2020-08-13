package com.zhigui.crossmesh.mesher.resource.fabric;

import com.google.gson.Gson;
import com.google.protobuf.ByteString;
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
import org.hyperledger.fabric.sdk.BlockEvent;
import org.hyperledger.fabric.sdk.BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import static com.zhigui.crossmesh.proto.Types.TransactionID;
import static com.zhigui.crossmesh.proto.Types.URI;

/**
 * Created by IntelliJ IDEA.
 * Author: kaichen
 * Date: 2020/7/21
 * Time: 5:18 PM
 */
public class FabricResource implements Resource {
    private String selfNetwork;

    private String baseUrl;

    private URI uri;

    private Network network;

    private ConcurrentHashMap<String, CompletableFuture<BlockEvent.TransactionEvent>> transactionResults;

    private Coordinator coordinator;

    public FabricResource(URI uri, byte[] connection, Coordinator coordinator) {
        this.coordinator = coordinator;
        this.selfNetwork = "";
        this.baseUrl = "";
        this.uri = uri;
        this.transactionResults = new ConcurrentHashMap<>();
        setConnection(connection);
    }

    @Override
    public CompletableFuture<BranchTransactionResponse> submitBranchTransaction(BranchTransaction branchTransaction) {
        return CompletableFuture.supplyAsync(() -> {
            String contractName = branchTransaction.getInvocation().getContract();
            if (network == null) {
                throw new RuntimeException("network service not found");
            }

            Contract contract = network.getContract(contractName);
            if (contract == null) {
                throw new RuntimeException("contract service not found");
            }
            if (selfNetwork.equals(uri.getNetwork())) {
                contract.addContractListener(new CrossContractListener("CROSS_CONTRACT_LISTENER", coordinator));
            }

            Transaction tx = contract.createTransaction(branchTransaction.getInvocation().getFunc());
            BranchTransactionResponse.Builder builder = BranchTransactionResponse.newBuilder();
            TransactionID branchTransactionId = TransactionID.newBuilder().setUri(this.uri).setId(tx.getTransactionId()).build();
            builder.setTxId(branchTransactionId);
            transactionResults.computeIfAbsent(network.getChannel() + tx.getTransactionId(), s -> new CompletableFuture<>());
            try {
                tx.submit(branchTransaction.getInvocation().getArgsList().toArray(new String[0]));
                builder.setStatus(BranchTransactionResponse.Status.SUCCESS);
            } catch (ContractException | TimeoutException | InterruptedException | GatewayRuntimeException e) {
                e.printStackTrace();
                builder.setStatus(BranchTransactionResponse.Status.FAILED);
            }
            try {
                TransactionActionInfo txInfo = transactionResults.get(network.getChannel() + tx.getTransactionId()).get(30, TimeUnit.SECONDS).getTransactionActionInfo(0);
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
                String proofStr = proofJson.toJson(proof);
                builder.setProof(ByteString.copyFromUtf8(proofStr));
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                e.printStackTrace();
                builder.setStatus(BranchTransactionResponse.Status.FAILED);
            }

            return builder.build();
        });
    }

    @Override
    public CompletableFuture<GlobalTransactionStatus> evaluateGlobalTransaction(String transactionId) {

        return null;
    }

    private void setConnection(byte[] connection) {
        Path idDir = Paths.get(this.baseUrl, this.uri.getNetwork(), this.uri.getChain());
        Wallet wallet;
        try {
            wallet = Wallets.newFileSystemWallet(idDir);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("identity dir not found");
        }

        Optional<String> identityOpt;
        try {
            identityOpt = wallet.list().stream().findFirst();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("identity dir list failed");
        }

        if (!identityOpt.isPresent()) {
            throw new RuntimeException("identity file not found");
        }

        Gateway.Builder builder;
        try {
            builder = Gateway.createBuilder()
                .identity(wallet, identityOpt.get())
                .networkConfig(new ByteArrayInputStream(connection));
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("identity file not found");
        }

        Gateway gateway = builder.connect();
        Network network = gateway.getNetwork(this.uri.getChain());
        if (network == null) {
            throw new RuntimeException("network service create failed");
        }
        this.network = network;
        this.network.addBlockListener(blockEvent -> blockEvent.getTransactionEvents().forEach(transactionEvent -> transactionResults.get(transactionEvent.getChannelId() + transactionEvent.getTransactionID()).complete(transactionEvent)));
    }
}
