package com.raftfs.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GcsChunkStore implements ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(GcsChunkStore.class);

    private final Storage storage;
    private final String bucketName;
    private final ChecksumValidator checksumValidator;

    public GcsChunkStore(String bucketName, String projectId) {
        this.storage = StorageOptions.newBuilder()
                .setProjectId(projectId)
                .build()
                .getService();
        this.bucketName = bucketName;
        this.checksumValidator = new ChecksumValidator();
        ensureBucketExists();
    }

    private void ensureBucketExists() {
        try {
            if (storage.get(bucketName) == null) {
                storage.create(BucketInfo.of(bucketName));
                log.info("Created GCS bucket: {}", bucketName);
            }
        } catch (StorageException e) {
            log.warn("Could not verify bucket existence: {}", e.getMessage());
        }
    }

    @Override
    public boolean write(String key, byte[] data) {
        try {
            long checksum = checksumValidator.compute(data);
            BlobId blobId = BlobId.of(bucketName, key);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
                    .setMetadata(Map.of("checksum", String.valueOf(checksum)))
                    .build();
            storage.create(blobInfo, data);
            return true;
        } catch (StorageException e) {
            log.error("Failed to write key {} to GCS: {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public byte[] read(String key) {
        try {
            Blob blob = storage.get(BlobId.of(bucketName, key));
            if (blob == null) return null;

            byte[] data = blob.getContent();
            String storedChecksum = blob.getMetadata() != null
                    ? blob.getMetadata().get("checksum") : null;

            if (storedChecksum != null) {
                long expected = Long.parseLong(storedChecksum);
                if (!checksumValidator.validate(data, expected)) {
                    log.error("Checksum mismatch for key {}", key);
                    return null;
                }
            }
            return data;
        } catch (StorageException e) {
            log.error("Failed to read key {} from GCS: {}", key, e.getMessage());
            return null;
        }
    }

    @Override
    public boolean delete(String key) {
        try {
            return storage.delete(BlobId.of(bucketName, key));
        } catch (StorageException e) {
            log.error("Failed to delete key {} from GCS: {}", key, e.getMessage());
            return false;
        }
    }

    @Override
    public boolean exists(String key) {
        try {
            Blob blob = storage.get(BlobId.of(bucketName, key));
            return blob != null && blob.exists();
        } catch (StorageException e) {
            return false;
        }
    }
}
