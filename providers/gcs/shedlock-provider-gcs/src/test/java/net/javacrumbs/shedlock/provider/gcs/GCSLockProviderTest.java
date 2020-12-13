package net.javacrumbs.shedlock.provider.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.test.support.AbstractLockProviderIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <b>Note</b>: If tests fail when build in your IDE first unpack native
 * dependencies by running
 * from project root and ensure that <code>sqlite4java.library.path</code>
 * is set to <code>./target/dependencies</code>.
 */
@Testcontainers
class GCSLockProviderTest  extends AbstractLockProviderIntegrationTest {

    private static final int HOVERFLY_SIMULATION_PORT = 8500;

    @Container
    public static GenericContainer gcs = new GenericContainer("spectolabs/hoverfly:latest")
        .withExposedPorts(HOVERFLY_SIMULATION_PORT)
        .withCommand("-webserver","-import","/var/hoverfly/simulation.json","-log-level","debug")
        .withClasspathResourceMapping("lala_1.json","/var/hoverfly/simulation.json" ,BindMode.READ_ONLY);

    private static final String BUCKET_NAME = "ShedLock";
    private static Storage storage;

    @BeforeAll
    static void setUpStorageClient() throws InterruptedException {
        System.out.println(LOCK_NAME1);
        String host= gcs.getHost();
        Integer mappedPort = gcs.getMappedPort(HOVERFLY_SIMULATION_PORT);
        storage = StorageOptions.newBuilder().setHost("http://"+host+":"+mappedPort).build().getService();
    }


    @AfterAll
    static void afterAll() {
        System.out.println("sd");
    }

    @Override
    protected LockProvider getLockProvider() {
        return new GCSLockProvider(storage, BUCKET_NAME);
    }

    @Override
    protected void assertUnlocked(String lockName) {
        Blob blob = storage.get(BlobId.of(BUCKET_NAME,lockName));
        assertThat(blob).isNull();
    }

    @Override
    protected void assertLocked(String lockName) {
        byte[] contents = storage.get(BlobId.of(BUCKET_NAME, lockName)).getContent();
        String stringValue = new String(contents);
        assertThat(stringValue).isNotNull();
    }

}
