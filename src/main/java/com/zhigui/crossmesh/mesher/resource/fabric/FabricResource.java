package com.zhigui.crossmesh.mesher.resource.fabric;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.zhigui.crossmesh.mesher.Config;
import com.zhigui.crossmesh.mesher.Coordinator;
import com.zhigui.crossmesh.mesher.resource.Resource;
import org.apache.commons.codec.binary.Hex;
import org.hyperledger.fabric.gateway.Contract;
import org.hyperledger.fabric.gateway.ContractException;
import org.hyperledger.fabric.gateway.Gateway;
import org.hyperledger.fabric.gateway.GatewayRuntimeException;
import org.hyperledger.fabric.gateway.Network;
import org.hyperledger.fabric.gateway.Transaction;
import org.hyperledger.fabric.gateway.Wallet;
import org.hyperledger.fabric.gateway.Wallets;
import org.hyperledger.fabric.protos.common.Common;
import org.hyperledger.fabric.protos.msp.Identities;
import org.hyperledger.fabric.protos.peer.ProposalPackage;
import org.hyperledger.fabric.protos.peer.ProposalResponsePackage;
import org.hyperledger.fabric.protos.peer.TransactionPackage;
import org.hyperledger.fabric.sdk.BlockEvent.TransactionEvent;
import org.hyperledger.fabric.sdk.BlockInfo.TransactionEnvelopeInfo.TransactionActionInfo;
import org.hyperledger.fabric.sdk.TransactionInfo;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.InvalidProtocolBufferRuntimeException;
import org.hyperledger.fabric.sdk.exception.ProposalException;
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

    private final String selfNetwork;

    private final String baseUrl;

    private final URI uri;

    private Network network;

    private final ConcurrentHashMap<String, CompletableFuture<TransactionEvent>> transactionResults;

    private final Coordinator coordinator;

    public FabricResource(URI uri, byte[] connection, Path connPath, Coordinator coordinator, Config config) {
        this.coordinator = coordinator;
        this.selfNetwork = config.getSelfNetwork();
        this.baseUrl = config.getIdBasePath();
        this.uri = uri;
        this.transactionResults = new ConcurrentHashMap<>();
        setConnection(connection, connPath);
    }

    @Override
    public CompletableFuture<BranchTransactionResponse> submitBranchTransaction(BranchTransaction branchTx) {
        return CompletableFuture.supplyAsync(() -> {
            String contractName = branchTx.getInvocation().getContract();
            if (network == null) {
                throw new RuntimeException("network service not found");
            }

            Contract contract = network.getContract(contractName);
            if (contract == null) {
                throw new RuntimeException("contract service not found");
            }
            if (!branchTx.getTxId().getUri().equals(this.uri)) {
                throw new RuntimeException("resource uri not match transaction uri");
            }

            Transaction tx = contract.createTransaction(branchTx.getInvocation().getFunc());
            BranchTransactionResponse.Builder builder = BranchTransactionResponse.newBuilder();
            TransactionID branchTransactionId = TransactionID.newBuilder().setUri(this.uri).setId(tx.getTransactionId()).build();
            builder.setTxId(branchTransactionId);
            try {
                tx.submit(branchTx.getInvocation().getArgsList().toArray(new String[0]));
                builder.setStatus(BranchTransactionResponse.Status.SUCCESS);
            } catch (ContractException | TimeoutException | InterruptedException | GatewayRuntimeException e) {
                LOGGER.error("submit transaction failed", e);
                builder.setProof("");
                builder.setStatus(BranchTransactionResponse.Status.FAILED);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return builder.build();
            }

            try {
                String proofStr = getProofForTransaction(tx.getTransactionId()).get(30, TimeUnit.SECONDS);
                builder.setProof(proofStr);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("get transaction proof failed", e);
                builder.setProof("");
                builder.setStatus(BranchTransactionResponse.Status.FAILED);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
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
        CompletableFuture<TransactionEvent> future = transactionResults.remove(txId);
        if (future != null) {
            return future.thenApply(transactionEvent -> {
                TransactionActionInfo txInfo = transactionEvent.getTransactionActionInfo(0);
                byte[] proposalResponsePayload = txInfo.getProposalResponsePayload();
                Proof proof = new Proof();
                proof.setProposalResponsePayload(ByteString.copyFrom(proposalResponsePayload).toStringUtf8());
                Set<EndorserInfo> endorserInfos = new HashSet<>();
                for (int i = 0; i < txInfo.getEndorsementsCount(); i++) {
                    EndorserInfo endorserInfo = new EndorserInfo();
                    endorserInfo.setId(txInfo.getEndorsementInfo(i).getId());
                    endorserInfo.setMspId(txInfo.getEndorsementInfo(i).getMspid());
                    endorserInfo.setSignature(Hex.encodeHexString(txInfo.getEndorsementInfo(i).getSignature()));
                    endorserInfos.add(endorserInfo);
                }
                proof.setEndorserInfos(endorserInfos);
                Gson proofJson = new Gson();
                return proofJson.toJson(proof);
            });
        } else {
            return CompletableFuture.supplyAsync(() -> {
                for (; ; ) {
                    try {
                        TransactionInfo txInfo = network.getChannel().queryTransactionByID(txId);
                        if (txInfo == null) {
                            Thread.sleep(1000);
                            continue;
                        }
                        Common.Envelope envelope = txInfo.getEnvelope();
                        Common.Payload payload = Common.Payload.parseFrom(envelope.getPayload());
                        TransactionPackage.Transaction transaction = TransactionPackage.Transaction.parseFrom(payload.getData());
                        TransactionPackage.TransactionAction action = transaction.getActionsList().get(0);
                        TransactionPackage.ChaincodeActionPayload chaincodeActionPayload = TransactionPackage.ChaincodeActionPayload.parseFrom(action.getPayload());
                        ProposalResponsePackage.ProposalResponsePayload prp = ProposalResponsePackage.ProposalResponsePayload.parseFrom(chaincodeActionPayload.getAction().getProposalResponsePayload());
                        ProposalPackage.ChaincodeAction ccaction = ProposalPackage.ChaincodeAction.parseFrom(prp.getExtension());

                        Proof proof = new Proof();
                        proof.setProposalResponsePayload(ByteString.copyFrom(ccaction.getResponse().getPayload().toByteArray()).toStringUtf8());
                        Set<EndorserInfo> endorserInfos = new HashSet<>();
                        chaincodeActionPayload.getAction().getEndorsementsList().forEach(endorsement -> {
                            EndorserInfo endorserInfo = new EndorserInfo();
                            try {
                                endorserInfo.setId(Identities.SerializedIdentity.parseFrom(endorsement.getEndorser()).getIdBytes().toStringUtf8());
                            } catch (InvalidProtocolBufferException e) {
                                throw new InvalidProtocolBufferRuntimeException(e);
                            }
                            try {
                                endorserInfo.setMspId(Identities.SerializedIdentity.parseFrom(endorsement.getEndorser()).getMspid());
                            } catch (InvalidProtocolBufferException e) {
                                throw new InvalidProtocolBufferRuntimeException(e);
                            }

                            endorserInfo.setSignature(Hex.encodeHexString(endorsement.getSignature().toByteArray()));
                            endorserInfos.add(endorserInfo);
                        });
                        proof.setEndorserInfos(endorserInfos);
                        Gson proofJson = new Gson();
                        return proofJson.toJson(proof);
                    } catch (ProposalException | InvalidArgumentException | InvalidProtocolBufferException | InterruptedException e) {
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        throw new RuntimeException(e);
                    }
                }
            });


        }

    }

    public void addTransactionEvent(String txID, TransactionEvent transactionEvent) {
        this.transactionResults.putIfAbsent(txID, CompletableFuture.completedFuture(transactionEvent));
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
        LOGGER.info(this.uri.getChain());
        Network network = gateway.getNetwork(this.uri.getChain());
        if (network == null) {
            throw new RuntimeException("network service create failed");
        }
        this.network = network;
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
                LOGGER.info(jsonElement.getAsString());
                if (contract == null) {
                    throw new RuntimeException("contract service not found");
                }
                contract.addContractListener(new CrossContractListener("CROSS_CONTRACT_LISTENER", coordinator, jsonElement.getAsString()));
            });
        }

    }
}
