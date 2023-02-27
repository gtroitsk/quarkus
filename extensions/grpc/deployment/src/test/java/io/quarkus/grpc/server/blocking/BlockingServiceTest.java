package io.quarkus.grpc.server.blocking;

import grpc.health.v1.HealthGrpc;
import grpc.health.v1.HealthOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.examples.helloworld.HelloWorldProto;
import io.grpc.reflection.v1.*;
import io.grpc.testing.integration.TestServiceGrpc;
import io.quarkus.grpc.GrpcClient;
import io.quarkus.grpc.runtime.health.GrpcHealthStorage;
import io.quarkus.grpc.server.services.BlockingMutinyHelloService;
import io.quarkus.test.QuarkusUnitTest;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class BlockingServiceTest {

    @RegisterExtension
    static final QuarkusUnitTest config = new QuarkusUnitTest()
            .setFlatClassPath(true)
            .setArchiveProducer(
                    () -> ShrinkWrap.create(JavaArchive.class)
                            .addPackage(HealthGrpc.class.getPackage())
                            .addPackage(GreeterGrpc.class.getPackage())
                            .addPackage(TestServiceGrpc.class.getPackage())
                            .addClasses(BlockingMutinyHelloService.class))
            .withConfigurationResource("reflection-config.properties");

    protected ManagedChannel channel;

    @Inject
    GrpcHealthStorage healthService;

    @GrpcClient("reflection-service")
    MutinyServerReflectionGrpc.MutinyServerReflectionStub reflection;

    @GrpcClient("test-service")
    TestServiceGrpc.TestServiceBlockingStub test;



    @BeforeEach
    public void init() {
        channel = ManagedChannelBuilder.forAddress("localhost", 9001)
                .usePlaintext()
                .build();
    }

    @AfterEach
    public void shutdown() {
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    @Test
    public void testInvokingABlockingService() {
        HelloReply reply = GreeterGrpc.newBlockingStub(channel)
                .sayHello(HelloRequest.newBuilder().setName("neo").build());
        assertThat(reply.getMessage()).contains("executor-thread", "neo");
    }

    @Test
    public void testHealth() {
        assertThat(healthService.getStatuses())
                .contains(entry("helloworld.Greeter", HealthOuterClass.HealthCheckResponse.ServingStatus.SERVING));
    }

    @Test
    public void testReflection_getServiceList() {
        ServerReflectionRequest request = ServerReflectionRequest.newBuilder().setHost("localhost")
                .setListServices("").build();

        ServerReflectionResponse response = invoke(request);
        List<ServiceResponse> list = response.getListServicesResponse().getServiceList();
        assertThat(list).hasSize(2)
                .anySatisfy(r -> assertThat(r.getName()).isEqualTo(GreeterGrpc.SERVICE_NAME))
                .anySatisfy(r -> assertThat(r.getName()).isEqualTo("grpc.health.v1.Health"));
    }

    @Test
    public void testReflection_serviceCount() {
        ServerReflectionRequest request = ServerReflectionRequest.newBuilder()
                .setListServices("").build();

        ServerReflectionResponse response = invoke(request);

        int numberOfServices = response.getListServicesResponse().getServiceCount();
        assertThat(numberOfServices).isEqualTo(3);
    }

    @Test
    public void testReflection_get() {
        ServerReflectionRequest request = ServerReflectionRequest.newBuilder()
                .setListServices("").build();

        ServerReflectionResponse response = invoke(request);

        List<ServiceResponse> list = response.getListServicesResponse().getServiceList();
        assertThat(list).hasSize(2)
                .anySatisfy(r -> assertThat(r.getName()).isEqualTo(GreeterGrpc.SERVICE_NAME))
                .anySatisfy(r -> assertThat(r.getName()).isEqualTo("grpc.health.v1.Health"));

        String[] splittedServiceName = list.get(0).getName().split("[.]");
        String protoFileName = splittedServiceName[0] + ".proto";

        // Request to server with given proto file name
        ServerReflectionRequest request2 = ServerReflectionRequest.newBuilder()
                .setHost("localhost")
                .setFileByFilename(protoFileName)
                .build();

        ServerReflectionResponse expected = ServerReflectionResponse.newBuilder()
                .setValidHost("localhost")
                .setOriginalRequest(request2)
                .setFileDescriptorResponse(
                        FileDescriptorResponse.newBuilder()
                                .addFileDescriptorProto(
                                        // TODO: get class name from service name
                                        HelloWorldProto.getDescriptor().toProto().toByteString())
                                .build())
                .build();

        ServerReflectionResponse response2 = invoke(request2);
        assertThat(response2).isEqualTo(expected);
    }

    private ServerReflectionResponse invoke(ServerReflectionRequest request) {
        return reflection.serverReflectionInfo(Multi.createFrom().item(request))
                .collect().first()
                .await().indefinitely();
    }
}
