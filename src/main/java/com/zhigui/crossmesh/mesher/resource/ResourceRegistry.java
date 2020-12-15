package com.zhigui.crossmesh.mesher.resource;

import com.zhigui.crossmesh.mesher.Config;
import com.zhigui.crossmesh.mesher.Coordinator;
import com.zhigui.crossmesh.mesher.resource.fabric.FabricResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;

import static com.zhigui.crossmesh.proto.Types.ResourceRegisteredOrUpdatedEvent;
import static com.zhigui.crossmesh.proto.Types.URI;

@Component
public class ResourceRegistry {
    private final ConcurrentHashMap<URI, Resource> resources = new ConcurrentHashMap<>();
    @Autowired
    private Config config;

    @Autowired
    private Coordinator coordinator;

    @PostConstruct
    public void start() {
        URI uri = URI.newBuilder().setNetwork(this.config.getMetaNetwork()).setChain(this.config.getMetaChain()).build();
        Resource resource = null;
        switch (config.getMetaChainType()) {
            case FABRIC:
                resource = new FabricResource(uri, null, Paths.get(this.config.getFabricMetaChainConn()), coordinator, this.config);
            case XUPERCHAIN:
            default:
                break;
        }
        if (resource != null) {
            this.resources.put(uri, resource);
        }
    }

    public Resource getResource(URI uri) {
        return resources.get(uri);
    }

    public void handleResourceRegisteredEvent(ResourceRegisteredOrUpdatedEvent resourceRegisteredEvent) {
        switch (resourceRegisteredEvent.getType()) {
            case FABRIC:
                this.resources.compute(resourceRegisteredEvent.getUri(), (uri,val) -> new FabricResource(uri, Base64.getDecoder().decode(resourceRegisteredEvent.getConnection().toStringUtf8()), null, coordinator, config));
                break;
            case XUPERCHAIN:
            case BCOS:
                break;
            default:
                throw new RuntimeException();
        }
    }
}
