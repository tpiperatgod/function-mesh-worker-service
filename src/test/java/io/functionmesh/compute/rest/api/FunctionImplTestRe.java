package io.functionmesh.compute.rest.api;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.functionmesh.compute.MeshWorkerService;
import io.kubernetes.client.openapi.ApiException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.functions.worker.WorkerConfig;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FunctionImplTestRe {

    private static final String tenant = "test-tenant";
    private static final String namespace = "test-namespace";
    private static final String function = "test-function";
    private static final String outputTopic = "test-output-topic";
    private static final String pulsarFunctionCluster = "test-pulsar";

    private MeshWorkerService meshWorkerService;
    private PulsarAdmin mockedPulsarAdmin;
    private Tenants mockedTenants;
    private Namespaces mockedNamespaces;
    private TenantInfo mockedTenantInfo;
    private Namespace mockedNamespace;
    private List<String> namespaceList = new LinkedList<>();
    private FunctionsImpl resource;

    @BeforeMethod
    public void setup() throws Exception {

        this.mockedTenantInfo = mock(TenantInfo.class);
        this.mockedPulsarAdmin = mock(PulsarAdmin.class);
        this.mockedNamespace = mock(Namespace.class);
        this.mockedTenants = mock(Tenants.class);
        this.mockedNamespaces = mock(Namespaces.class);
        namespaceList.add(tenant + "/" + namespace);

        when(mockedPulsarAdmin.tenants()).thenReturn(mockedTenants);
        when(mockedPulsarAdmin.namespaces()).thenReturn(mockedNamespaces);
        when(mockedTenants.getTenantInfo(any())).thenReturn(mockedTenantInfo);
        when(mockedNamespaces.getNamespaces(any())).thenReturn(namespaceList);
        WorkerConfig workerConfig = mockWorkerConfig();

        this.meshWorkerService = mock(MeshWorkerService.class);
        when(meshWorkerService.getWorkerConfig()).thenReturn(workerConfig);
        when(meshWorkerService.isInitialized()).thenReturn(true);
        when(meshWorkerService.getBrokerAdmin()).thenReturn(mockedPulsarAdmin);

        this.resource = spy(new FunctionsImpl(() -> this.meshWorkerService));
    }

    private WorkerConfig mockWorkerConfig() {
        WorkerConfig workerConfig = mock(WorkerConfig.class);
        when(workerConfig.isAuthorizationEnabled()).thenReturn(false);
        when(workerConfig.isAuthenticationEnabled()).thenReturn(false);
        when(workerConfig.getPulsarFunctionsCluster()).thenReturn(pulsarFunctionCluster);
        return workerConfig;
    }

    private PulsarAdmin mockPulsarAdmin() {

        return mockedPulsarAdmin;
    }

    @Test
    public void getFunctionStatsTest() throws ApiException, IOException {
        this.resource.getFunctionStats(tenant, namespace, function, null, null, null);
    }
}
