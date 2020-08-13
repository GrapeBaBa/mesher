package com.zhigui.crossmesh.mesher.resource;

import com.zhigui.crossmesh.mesher.Coordinator;
import com.zhigui.crossmesh.mesher.resource.fabric.FabricResource;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

import static com.zhigui.crossmesh.proto.Types.ResourceRegisteredEvent;
import static com.zhigui.crossmesh.proto.Types.URI;

@Component
public class ResourceRegistry {

    private final ConcurrentHashMap<URI, Resource> resources;

    public ResourceRegistry() {
        this.resources = new ConcurrentHashMap<>();
    }

    public Resource getResource(URI uri) {
        return resources.get(uri);
    }

    public void handleResourceRegisteredEvent(ResourceRegisteredEvent resourceRegisteredEvent, Coordinator coordinator) {
        switch (resourceRegisteredEvent.getType()) {
            case FABRIC:
                this.resources.computeIfAbsent(resourceRegisteredEvent.getUri(), uri -> new FabricResource(uri, resourceRegisteredEvent.getConnection().toByteArray(), coordinator));
                break;
            case XUPERCHAIN:
            case BCOS:
                break;
            default:
                throw new RuntimeException();
        }
    }
}
