/**
 * Copyright 2009-2020 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.javacrumbs.shedlock.provider.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonObject;
import net.javacrumbs.shedlock.core.AbstractSimpleLock;
import net.javacrumbs.shedlock.core.ClockProvider;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.core.SimpleLock;
import net.javacrumbs.shedlock.support.LockException;
import net.javacrumbs.shedlock.support.Utils;
import net.javacrumbs.shedlock.support.annotation.NonNull;

import java.time.Instant;
import java.util.Optional;

import static net.javacrumbs.shedlock.support.Utils.toIsoString;

/**
 * Distributed lock using Google Cloud Storage.
 * Depends on <code>google-cloud-storage</code>.
 * <p>
 * It uses a bucket and an json object with the following structure
 * <pre>
 * {
 *    "_id" : "lock name",
 *    "lockUntil" : ISODate("2017-01-07T16:52:04.071Z"),
 *    "lockedAt" : ISODate("2017-01-07T16:52:03.932Z"),
 *    "lockedBy" : "host name"
 * }
 * </pre>
 *
 * <code>lockedAt</code> and <code>lockedBy</code> are just for troubleshooting
 * and are not read by the code.
 *
 * <ol>
 * <li>
 * Attempts to insert the lock file.
 * </li>
 * <li>
 * If the lock file creation succeeded, we have the lock. If the update failed (condition check exception)
 * somebody else holds the lock.
 * </li>
 * <li>
 * When unlocking, the storage object is moved to another store object.
 * </li>
 * </ol>
 */
public class GCSLockProvider implements LockProvider {
    static final String LOCK_UNTIL = "lockUntil";
    static final String LOCKED_AT = "lockedAt";
    static final String LOCKED_BY = "lockedBy";
    static final String ID = "_id";

    private final Storage storage;
    private final String bucketName;
    private final String hostname;

    public GCSLockProvider(@NonNull Storage storage, @NonNull String bucketName) {
        this.storage = storage;
        this.bucketName = bucketName;
        this.hostname = Utils.getHostname();
    }

    @Override
    public Optional<SimpleLock> lock(LockConfiguration lockConfiguration) {
        String nowIso = toIsoString(now());
        String lockUntilIso = toIsoString(lockConfiguration.getLockAtMostUntil());

        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty(LOCK_UNTIL,lockUntilIso);
        jsonObject.addProperty(LOCKED_AT, nowIso);
        jsonObject.addProperty(LOCKED_BY, hostname);
        jsonObject.addProperty(ID, lockConfiguration.getName());
        String jsonString = jsonObject.toString();

        try {
            BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, lockConfiguration.getName()).build();
            Blob blob = storage.create(blobInfo, jsonString.getBytes(), Storage.BlobTargetOption.doesNotExist(),
                Storage.BlobTargetOption.disableGzipContent());
            return Optional.of(new GCSLock(storage, blob, lockConfiguration));
        } catch (StorageException storageException) {
            //StorageException. This means there is a file already located
            return Optional.empty();
        }
    }

    private Instant now() {
        return ClockProvider.now();
    }

    private static final class GCSLock extends AbstractSimpleLock {

        public static final String UNLOCK_SUFFIX = ".unlocked";
        private final Storage storage;
        private final Blob blob;

        public GCSLock(Storage storage, Blob blob, LockConfiguration lockConfiguration) {
            super(lockConfiguration);
            this.storage = storage;
            this.blob = blob;
        }

        @Override
        protected void doUnlock() {
            BlobId from = blob.getBlobId();
            BlobId to = BlobId.of(from.getBucket(),from.getName()+ UNLOCK_SUFFIX);

            Storage.CopyRequest copyRequest = Storage.CopyRequest.of(blob.getBlobId(), to);

            Blob copied = storage.copy(copyRequest).getResult();
            boolean deleted = storage.delete(from);
            if(!deleted) {
                storage.delete(copied.getBlobId());
                throw new LockException("Could not unlock lock file: "+from+" was not deleted");
            }
        }

    }

}
