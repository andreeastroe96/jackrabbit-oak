/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.azure.core.http.rest.Response;
import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobContainerProperties;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.Block;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.BlockListType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobUploadFromFileOptions;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.microsoft.azure.storage.RetryPolicy;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import com.google.common.base.Strings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.AbstractDataRecord;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;

import static java.lang.Thread.currentThread;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.BUFFERED_STREAM_THRESHOLD;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.DEFAULT_CONCURRENT_REQUEST_COUNT;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.LAST_MODIFIED_KEY;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.MAX_ALLOWABLE_UPLOAD_URIS;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.MAX_BINARY_UPLOAD_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.MAX_CONCURRENT_REQUEST_COUNT;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.MAX_MULTIPART_UPLOAD_PART_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.MAX_SINGLE_PUT_UPLOAD_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.META_DIR_NAME;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.META_KEY_PREFIX;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.MIN_MULTIPART_UPLOAD_PART_SIZE;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Constants.REF_KEY;


public class AzureBlobStoreBackend extends AbstractAzureBlobStoreBackend {

    private static final Logger LOG = LoggerFactory.getLogger(AzureBlobStoreBackend.class);
    private static final Logger LOG_STREAMS_DOWNLOAD = LoggerFactory.getLogger("oak.datastore.download.streams");
    private static final Logger LOG_STREAMS_UPLOAD = LoggerFactory.getLogger("oak.datastore.upload.streams");

    private Properties properties;
    private AzureBlobContainerProvider azureBlobContainerProvider;
    private int concurrentRequestCount = DEFAULT_CONCURRENT_REQUEST_COUNT;
    private RequestRetryOptions retryOptions;
    private Integer requestTimeout;
    private int httpDownloadURIExpirySeconds = 0; // disabled by default
    private int httpUploadURIExpirySeconds = 0; // disabled by default
    private String uploadDomainOverride = null;
    private String downloadDomainOverride = null;
    private boolean createBlobContainer = true;
    private boolean presignedDownloadURIVerifyExists = true;

    private Cache<String, URI> httpDownloadURICache;

    private byte[] secret;

    public void setProperties(final Properties properties) {
        this.properties = properties;
    }
    private volatile BlobContainerClient azureContainer = null;

    protected BlobContainerClient getAzureContainer() throws DataStoreException {
        if (azureContainer == null) {
            synchronized (this) {
                if (azureContainer == null) {
                    azureContainer = azureBlobContainerProvider.getBlobContainer(retryOptions, properties);
                }
            }
        }
        return azureContainer;
    }

    @Override
    public void init() throws DataStoreException {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            LOG.debug("Started backend initialization");

            if (null == properties) {
                try {
                    properties = Utils.readConfig(Utils.DEFAULT_CONFIG_FILE);
                } catch (IOException e) {
                    throw new DataStoreException("Unable to initialize Azure Data Store from " + Utils.DEFAULT_CONFIG_FILE, e);
                }
            }

            try {
                createBlobContainer = PropertiesUtil.toBoolean(
                        Strings.emptyToNull(properties.getProperty(AzureConstants.AZURE_CREATE_CONTAINER)), true);
                initAzureDSConfig();

                concurrentRequestCount = PropertiesUtil.toInteger(
                        properties.getProperty(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION),
                        DEFAULT_CONCURRENT_REQUEST_COUNT);
                if (concurrentRequestCount < DEFAULT_CONCURRENT_REQUEST_COUNT) {
                    LOG.warn("Invalid setting [{}] for concurrentRequestsPerOperation (too low); resetting to {}",
                            concurrentRequestCount,
                            DEFAULT_CONCURRENT_REQUEST_COUNT);
                    concurrentRequestCount = DEFAULT_CONCURRENT_REQUEST_COUNT;
                } else if (concurrentRequestCount > MAX_CONCURRENT_REQUEST_COUNT) {
                    LOG.warn("Invalid setting [{}] for concurrentRequestsPerOperation (too high); resetting to {}",
                            concurrentRequestCount,
                            MAX_CONCURRENT_REQUEST_COUNT);
                    concurrentRequestCount = MAX_CONCURRENT_REQUEST_COUNT;
                }
                LOG.info("Using concurrentRequestsPerOperation={}", concurrentRequestCount);

                if (properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT) != null) {
                    requestTimeout = PropertiesUtil.toInteger(properties.getProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT), RetryPolicy.DEFAULT_CLIENT_RETRY_COUNT);
                }

                retryOptions = Utils.getRetryOptions(properties.getProperty(AzureConstants.AZURE_BLOB_MAX_REQUEST_RETRY), requestTimeout, computeSecondaryLocationEndpoint());

                presignedDownloadURIVerifyExists = PropertiesUtil.toBoolean(
                        Strings.emptyToNull(properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS)), true);



                BlobContainerClient azureContainer = getAzureContainer();

                if (createBlobContainer && !azureContainer.exists()) {
                    azureContainer.create();
                    LOG.info("New container created. containerName={}", getContainerName());
                } else {
                    LOG.info("Reusing existing container. containerName={}", getContainerName());
                }
                LOG.debug("Backend initialized. duration={}", (System.currentTimeMillis() - start));

                // settings pertaining to DataRecordAccessProvider functionality
                String putExpiry = properties.getProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS);
                if (null != putExpiry) {
                    this.setHttpUploadURIExpirySeconds(Integer.parseInt(putExpiry));
                }
                String getExpiry = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS);
                if (null != getExpiry) {
                    this.setHttpDownloadURIExpirySeconds(Integer.parseInt(getExpiry));
                    String cacheMaxSize = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE);
                    if (null != cacheMaxSize) {
                        this.setHttpDownloadURICacheSize(Integer.parseInt(cacheMaxSize));
                    } else {
                        this.setHttpDownloadURICacheSize(0); // default
                    }
                }
                uploadDomainOverride = properties.getProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE, null);
                downloadDomainOverride = properties.getProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE, null);

                // Initialize reference key secret
                boolean createRefSecretOnInit = PropertiesUtil.toBoolean(
                        Strings.emptyToNull(properties.getProperty(AzureConstants.AZURE_REF_ON_INIT)), true);

                if (createRefSecretOnInit) {
                    getOrCreateReferenceKey();
                }
            } catch (BlobStorageException e) {
                throw new DataStoreException(e);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    private void initAzureDSConfig() {
        AzureBlobContainerProvider.Builder builder = AzureBlobContainerProvider.Builder.builder(properties.getProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME))
                .withAzureConnectionString(properties.getProperty(AzureConstants.AZURE_CONNECTION_STRING, ""))
                .withAccountName(properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, ""))
                .withBlobEndpoint(properties.getProperty(AzureConstants.AZURE_BLOB_ENDPOINT, ""))
                .withSasToken(properties.getProperty(AzureConstants.AZURE_SAS, ""))
                .withAccountKey(properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, ""))
                .withTenantId(properties.getProperty(AzureConstants.AZURE_TENANT_ID, ""))
                .withClientId(properties.getProperty(AzureConstants.AZURE_CLIENT_ID, ""))
                .withClientSecret(properties.getProperty(AzureConstants.AZURE_CLIENT_SECRET, ""));
        azureBlobContainerProvider = builder.build();
    }

    @Override
    public InputStream read(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(
                    getClass().getClassLoader());
            BlockBlobClient blob = getAzureContainer().getBlobClient(key).getBlockBlobClient();
            if (!blob.exists()) {
                throw new DataStoreException(String.format("Trying to read missing blob. identifier=%s", key));
            }

            InputStream is = blob.openInputStream();
            LOG.debug("Got input stream for blob. identifier={} duration={}", key, (System.currentTimeMillis() - start));
            if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                // Log message, with exception, so we can get a trace to see where the call came from
                LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={}", key, new Exception());
            }
            return is;
        } catch (BlobStorageException e) {
            LOG.info("Error reading blob. identifier={}", key);
            throw new DataStoreException(String.format("Cannot read blob. identifier=%s", key), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private void uploadBlob(BlockBlobClient client, File file, long len, long start, String key) throws IOException {

        boolean useBufferedStream = len < BUFFERED_STREAM_THRESHOLD;
        try (InputStream in = useBufferedStream ?
                new BufferedInputStream(new FileInputStream(file))
                : new FileInputStream(file)) {

            ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
                    .setBlockSizeLong(len)
                    .setMaxConcurrency(concurrentRequestCount)
                    .setMaxSingleUploadSizeLong(MAX_SINGLE_PUT_UPLOAD_SIZE);
            BlobUploadFromFileOptions options = new BlobUploadFromFileOptions(file.toString());
            options.setParallelTransferOptions(parallelTransferOptions);
            try {
                BlobClient blobClient = client.getContainerClient().getBlobClient(file.getName());
                Response<BlockBlobItem> blockBlob = blobClient.uploadFromFileWithResponse(options, null, null);
                LOG.debug("Upload status is {} for blob {}", blockBlob.getStatusCode(), key);
            } catch (UncheckedIOException ex) {
                System.err.printf("Failed to upload from file: %s%n", ex.getMessage());
            }
            LOG.debug("Blob created. identifier={} length={} duration={} buffered={}", key, len, (System.currentTimeMillis() - start), useBufferedStream);
            if (LOG_STREAMS_UPLOAD.isDebugEnabled()) {
                // Log message, with exception, so we can get a trace to see where the call came from
                LOG_STREAMS_UPLOAD.debug("Binary uploaded to Azure Blob Storage - identifier={}", key, new Exception());
            }
        }
    }

    @Override
    public void write(DataIdentifier identifier, File file) throws DataStoreException {
        if (identifier == null) {
            throw new NullPointerException("identifier");
        }
        if (file == null) {
            throw new NullPointerException("file");
        }
        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            long len = file.length();
            LOG.debug("Blob write started. identifier={} length={}", key, len);
            BlockBlobClient blob = getAzureContainer().getBlobClient(key).getBlockBlobClient();
            if (!blob.exists()) {
                updateLastModifiedMetadata(blob);
                uploadBlob(blob, file, len, start, key);
                return;
            }

            if (blob.getProperties().getBlobSize() != len) {
                throw new DataStoreException("Length Collision. identifier=" + key +
                        " new length=" + len +
                        " old length=" + blob.getProperties().getBlobSize());
            }

            LOG.trace("Blob already exists. identifier={} lastModified={}", key, getLastModified(blob));
            updateLastModifiedMetadata(blob);

            LOG.debug("Blob updated. identifier={} lastModified={} duration={}", key,
                    getLastModified(blob), (System.currentTimeMillis() - start));
        } catch (BlobStorageException e) {
            LOG.info("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        } catch (IOException e) {
            LOG.debug("Error writing blob. identifier={}", key, e);
            throw new DataStoreException(String.format("Cannot write blob. identifier=%s", key), e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public DataRecord getRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) {
            throw new NullPointerException("identifier");
        }
        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            BlockBlobClient blob = getAzureContainer().getBlobClient(key).getBlockBlobClient();
            AzureBlobStoreDataRecord record = new AzureBlobStoreDataRecord(
                    this,
                    azureBlobContainerProvider,
                    new DataIdentifier(getIdentifierName(blob.getBlobName())),
                    getLastModified(blob),
                    blob.getProperties().getBlobSize());
            LOG.debug("Data record read for blob. identifier={} duration={} record={}",
                    key, (System.currentTimeMillis() - start), record);
            return record;
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                LOG.debug("Unable to get record for blob; blob does not exist. identifier={}", key);
            } else {
                LOG.info("Error getting data record for blob. identifier={}", key, e);
            }
            throw new DataStoreException(String.format("Cannot retrieve blob. identifier=%s", key), e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public Iterator<DataIdentifier> getAllIdentifiers() throws DataStoreException {
        return new RecordsIterator<>(
                input -> new DataIdentifier(getIdentifierName(input.getName())), getAzureContainer());
    }

    @Override
    public Iterator<DataRecord> getAllRecords() throws DataStoreException {
        final AbstractSharedBackend backend = this;
        return new RecordsIterator<>(
                input -> new AzureBlobStoreDataRecord(
                        backend,
                        azureBlobContainerProvider,
                        new DataIdentifier(getIdentifierName(input.getName())),
                        input.getLastModified(),
                        input.getLength()), getAzureContainer()
        );
    }

    @Override
    public boolean exists(DataIdentifier identifier) throws DataStoreException {
        long start = System.currentTimeMillis();
        String key = getKeyName(identifier);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            boolean exists = getAzureContainer().getBlobClient(key).getBlockBlobClient().exists();
            LOG.debug("Blob exists={} identifier={} duration={}", exists, key, (System.currentTimeMillis() - start));
            return exists;
        } catch (Exception e) {
            throw new DataStoreException(e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void close(){
        azureBlobContainerProvider.close();
        LOG.info("AzureBlobBackend closed.");
    }

    @Override
    public void deleteRecord(DataIdentifier identifier) throws DataStoreException {
        if (null == identifier) throw new NullPointerException("identifier");

        String key = getKeyName(identifier);
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            boolean result = getAzureContainer().getBlobClient(key).getBlockBlobClient().deleteIfExists();
            LOG.debug("Blob {}. identifier={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    key, (System.currentTimeMillis() - start));
        } catch (BlobStorageException e) {
            LOG.info("Error deleting blob. identifier={}", key, e);
            throw new DataStoreException(e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(InputStream input, String name) throws DataStoreException {
        if (null == input) {
            throw new NullPointerException("input");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name");
        }
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(input, name, -1);
            LOG.debug("Metadata record added. metadataName={} duration={}", name, (System.currentTimeMillis() - start));
        }
        finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public void addMetadataRecord(File input, String name) throws DataStoreException {
        if (input == null) {
            throw new NullPointerException("input");
        }
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name");
        }
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            addMetadataRecordImpl(new FileInputStream(input), name, input.length());
            LOG.debug("Metadata record added. metadataName={} duration={}", name, (System.currentTimeMillis() - start));
        } catch (FileNotFoundException e) {
            throw new DataStoreException(e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private BlockBlobClient getMetaBlobClient(String name) throws DataStoreException {
        return getAzureContainer().getBlobClient(META_DIR_NAME + "/" + name).getBlockBlobClient();
    }

    private void addMetadataRecordImpl(final InputStream input, String name, long recordLength) throws DataStoreException {
        try {
            BlockBlobClient blockBlobClient = getMetaBlobClient(name);
            updateLastModifiedMetadata(blockBlobClient);
            blockBlobClient.upload(BinaryData.fromBytes(input.readAllBytes()));
        } catch (BlobStorageException e) {
            LOG.info("Error adding metadata record. metadataName={} length={}", name, recordLength, e);
            throw new DataStoreException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataRecord getMetadataRecord(String name) {
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        long start = System.currentTimeMillis();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            BlockBlobClient blockBlobClient = getMetaBlobClient(name);
            if (!blockBlobClient.exists()) {
                LOG.warn("Trying to read missing metadata. metadataName={}", name);
                return null;
            }

            long lastModified = getLastModified(blockBlobClient);
            long length = blockBlobClient.getProperties().getBlobSize();
            AzureBlobStoreDataRecord record = new AzureBlobStoreDataRecord(this,
                    azureBlobContainerProvider,
                    new DataIdentifier(name),
                    lastModified,
                    length,
                    true);
            LOG.debug("Metadata record read. metadataName={} duration={} record={}", name, (System.currentTimeMillis() - start), record);
            return record;
        } catch (BlobStorageException e) {
            LOG.info("Error reading metadata record. metadataName={}", name, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            LOG.debug("Error reading metadata record. metadataName={}", name, e);
            throw new RuntimeException(e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public List<DataRecord> getAllMetadataRecords(String prefix) {
        if (null == prefix) {
            throw new NullPointerException("prefix");
        }
        long start = System.currentTimeMillis();
        final List<DataRecord> records = new ArrayList<>();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(META_DIR_NAME);

            for (BlobItem blobItem : getAzureContainer().listBlobs(listBlobsOptions, null)) {
                BlobClient blobClient = getAzureContainer().getBlobClient(blobItem.getName());
                BlobProperties properties = blobClient.getProperties();

                records.add(new AzureBlobStoreDataRecord(this,
                        azureBlobContainerProvider,
                        new DataIdentifier(stripMetaKeyPrefix(blobClient.getBlobName())),
                        getLastModified(blobClient.getBlockBlobClient()),
                        properties.getBlobSize(),
                        true));
            }
            LOG.debug("Metadata records read. recordsRead={} metadataFolder={} duration={}", records.size(), prefix, (System.currentTimeMillis() - start));
        } catch (BlobStorageException e) {
            LOG.info("Error reading all metadata records. metadataFolder={}", prefix, e);
        } catch (DataStoreException e) {
            LOG.debug("Error reading all metadata records. metadataFolder={}", prefix, e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return records;
    }

    @Override
    public boolean deleteMetadataRecord(String name) {
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            BlobClient blob = getAzureContainer().getBlobClient(addMetaKeyPrefix(name));
            boolean result = blob.deleteIfExists();
            LOG.debug("Metadata record {}. metadataName={} duration={}",
                    result ? "deleted" : "delete requested, but it does not exist (perhaps already deleted)",
                    name, (System.currentTimeMillis() - start));
            return result;
        } catch (BlobStorageException e) {
            LOG.info("Error deleting metadata record. metadataName={}", name, e);
        } catch (DataStoreException e) {
            LOG.debug("Error deleting metadata record. metadataName={}", name, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
    }

    @Override
    public void deleteAllMetadataRecords(String prefix) {
        if (null == prefix) {
            throw new NullPointerException("prefix");
        }
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

            int total = 0;

            ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
            listBlobsOptions.setPrefix(META_DIR_NAME);

            for (BlobItem blobItem : getAzureContainer().listBlobs(listBlobsOptions, null)) {
                BlobClient blobClient = getAzureContainer().getBlobClient(blobItem.getName());
                if (blobClient.deleteIfExists()) {
                    total++;
                }
            }
            LOG.debug("Metadata records deleted. recordsDeleted={} metadataFolder={} duration={}",
                    total, prefix, (System.currentTimeMillis() - start));

        } catch (BlobStorageException e) {
            LOG.info("Error deleting all metadata records. metadataFolder={}", prefix, e);
        } catch (DataStoreException e) {
            LOG.debug("Error deleting all metadata records. metadataFolder={}", prefix, e);
        } finally {
            if (null != contextClassLoader) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    @Override
    public boolean metadataRecordExists(String name) {
        long start = System.currentTimeMillis();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            BlobClient blob = getAzureContainer().getBlobClient(addMetaKeyPrefix(name));
            boolean exists = blob.exists();
            LOG.debug("Metadata record {} exists {}. duration={}", name, exists, (System.currentTimeMillis() - start));
            return exists;
        } catch (DataStoreException | BlobStorageException e) {
            LOG.debug("Error checking existence of metadata record = {}", name, e);
        } finally {
            if (contextClassLoader != null) {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
        return false;
    }

    /**
     * Get key from data identifier. Object is stored with key in ADS.
     */
    private static String getKeyName(DataIdentifier identifier) {
        String key = identifier.toString();
        return key.substring(0, 4) + Utils.DASH + key.substring(4);
    }

    /**
     * Get data identifier from key.
     */
    private static String getIdentifierName(String key) {
        if (!key.contains(Utils.DASH)) {
            return null;
        } else if (key.contains(META_KEY_PREFIX)) {
            return key;
        }
        return key.substring(0, 4) + key.substring(5);
    }

    private static String addMetaKeyPrefix(final String key) {
        return META_KEY_PREFIX + key;
    }

    private static String stripMetaKeyPrefix(String name) {
        if (name.startsWith(META_KEY_PREFIX)) {
            return name.substring(META_KEY_PREFIX.length());
        }
        return name;
    }

    private static void updateLastModifiedMetadata(BlockBlobClient blockBlobClient) {
        BlobContainerClient blobContainerClient = blockBlobClient.getContainerClient();
        Map<String, String> metadata = blobContainerClient.getProperties().getMetadata();
        metadata.put(LAST_MODIFIED_KEY, String.valueOf(System.currentTimeMillis()));
        blobContainerClient.setMetadata(metadata);
    }

    private static long getLastModified(BlockBlobClient blobClient) {
        BlobContainerProperties blobProperties = blobClient.getContainerClient().getProperties();
        if (blobProperties.getMetadata().containsKey(LAST_MODIFIED_KEY)) {
            return Long.parseLong(blobProperties.getMetadata().get(LAST_MODIFIED_KEY));
        }
        return blobProperties.getLastModified().toInstant().toEpochMilli();
    }

    protected void setHttpDownloadURIExpirySeconds(int seconds) {
        httpDownloadURIExpirySeconds = seconds;
    }

    protected void setHttpDownloadURICacheSize(int maxSize) {
        // max size 0 or smaller is used to turn off the cache
        if (maxSize > 0) {
            LOG.info("presigned GET URI cache enabled, maxSize = {} items, expiry = {} seconds", maxSize, httpDownloadURIExpirySeconds / 2);
            httpDownloadURICache = CacheBuilder.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfterWrite(httpDownloadURIExpirySeconds / 2, TimeUnit.SECONDS)
                    .build();
        } else {
            LOG.info("presigned GET URI cache disabled");
            httpDownloadURICache = null;
        }
    }

    protected URI createHttpDownloadURI(@NotNull DataIdentifier identifier,
                                        @NotNull DataRecordDownloadOptions downloadOptions) {
        URI uri = null;

        // When running unit test from Maven, it doesn't always honor the @NotNull decorators
        if (identifier == null) throw new NullPointerException("identifier");
        if (downloadOptions == null) throw new NullPointerException("downloadOptions");

        if (httpDownloadURIExpirySeconds > 0) {

            String domain = getDirectDownloadBlobStorageDomain(downloadOptions.isDomainOverrideIgnored());
            if (null == domain) {
                throw new NullPointerException("Could not determine domain for direct download");
            }

            String cacheKey = identifier.toString()
                    + domain
                    + Objects.toString(downloadOptions.getContentTypeHeader(), "")
                    + Objects.toString(downloadOptions.getContentDispositionHeader(), "");
            if (null != httpDownloadURICache) {
                uri = httpDownloadURICache.getIfPresent(cacheKey);
            }
            if (null == uri) {
                if (presignedDownloadURIVerifyExists) {
                    // Check if this identifier exists.  If not, we want to return null
                    // even if the identifier is in the download URI cache.
                    try {
                        if (!exists(identifier)) {
                            LOG.warn("Cannot create download URI for nonexistent blob {}; returning null", getKeyName(identifier));
                            return null;
                        }
                    } catch (DataStoreException e) {
                        LOG.warn("Cannot create download URI for blob {} (caught DataStoreException); returning null", getKeyName(identifier), e);
                        return null;
                    }
                }

                String key = getKeyName(identifier);

                uri = createPresignedURI(key,
                        new BlobSasPermission().setReadPermission(true),
                        httpDownloadURIExpirySeconds,
                        domain);
                if (uri != null && httpDownloadURICache != null) {
                    httpDownloadURICache.put(cacheKey, uri);
                }
            }
        }
        return uri;
    }

    protected void setHttpUploadURIExpirySeconds(int seconds) {
        httpUploadURIExpirySeconds = seconds;
    }

    private DataIdentifier generateSafeRandomIdentifier() {
        return new DataIdentifier(
                String.format("%s-%d",
                        UUID.randomUUID(),
                        Instant.now().toEpochMilli()
                )
        );
    }


    protected DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs, @NotNull final DataRecordUploadOptions options) {
        List<URI> uploadPartURIs = Lists.newArrayList();
        long minPartSize = MIN_MULTIPART_UPLOAD_PART_SIZE;
        long maxPartSize = MAX_MULTIPART_UPLOAD_PART_SIZE;

        if (0L >= maxUploadSizeInBytes) {
            throw new IllegalArgumentException("maxUploadSizeInBytes must be > 0");
        } else if (0 == maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        } else if (-1 > maxNumberOfURIs) {
            throw new IllegalArgumentException("maxNumberOfURIs must either be > 0 or -1");
        } else if (maxUploadSizeInBytes > MAX_SINGLE_PUT_UPLOAD_SIZE &&
                maxNumberOfURIs == 1) {
            throw new IllegalArgumentException(
                    String.format("Cannot do single-put upload with file size %d - exceeds max single-put upload size of %d",
                            maxUploadSizeInBytes,
                            MAX_SINGLE_PUT_UPLOAD_SIZE)
            );
        } else if (maxUploadSizeInBytes > MAX_BINARY_UPLOAD_SIZE) {
            throw new IllegalArgumentException(
                    String.format("Cannot do upload with file size %d - exceeds max upload size of %d",
                            maxUploadSizeInBytes,
                            MAX_BINARY_UPLOAD_SIZE)
            );
        }

        DataIdentifier newIdentifier = generateSafeRandomIdentifier();
        String blobId = getKeyName(newIdentifier);
        String uploadId = null;

        if (httpUploadURIExpirySeconds > 0) {
            // Always do multi-part uploads for Azure, even for small binaries.
            //
            // This is because Azure requires a unique header, "x-ms-blob-type=BlockBlob", to be
            // set but only for single-put uploads, not multi-part.
            // This would require clients to know not only the type of service provider being used
            // but also the type of upload (single-put vs multi-part), which breaks abstraction.
            // Instead we can insist that clients always do multi-part uploads to Azure, even
            // if the multi-part upload consists of only one upload part.  This doesn't require
            // additional work on the part of the client since the "complete" request must always
            // be sent regardless, but it helps us avoid the client having to know what type
            // of provider is being used, or us having to instruct the client to use specific
            // types of headers, etc.

            // Azure doesn't use upload IDs like AWS does
            // Generate a fake one for compatibility - we use them to determine whether we are
            // doing multi-part or single-put upload
            uploadId = Base64.encode(UUID.randomUUID().toString());

            long numParts = 0L;
            if (maxNumberOfURIs > 0) {
                long requestedPartSize = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) maxNumberOfURIs));
                if (requestedPartSize <= maxPartSize) {
                    numParts = Math.min(
                            maxNumberOfURIs,
                            Math.min(
                                    (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) minPartSize)),
                                    MAX_ALLOWABLE_UPLOAD_URIS
                            )
                    );
                } else {
                    throw new IllegalArgumentException(
                            String.format("Cannot do multi-part upload with requested part size %d", requestedPartSize)
                    );
                }
            } else {
                long maximalNumParts = (long) Math.ceil(((double) maxUploadSizeInBytes) / ((double) MIN_MULTIPART_UPLOAD_PART_SIZE));
                numParts = Math.min(maximalNumParts, MAX_ALLOWABLE_UPLOAD_URIS);
            }

            String key = getKeyName(newIdentifier);
            String domain = getDirectUploadBlobStorageDomain(options.isDomainOverrideIgnored());
            if (null == domain) {
                throw new NullPointerException("Could not determine domain for direct upload");
            }

            BlobSasPermission perms = new BlobSasPermission()
                    .setWritePermission(true);
            Map<String, String> presignedURIRequestParams = Maps.newHashMap();
            // see https://docs.microsoft.com/en-us/rest/api/storageservices/put-block#uri-parameters
            presignedURIRequestParams.put("comp", "block");
            for (long blockId = 1; blockId <= numParts; ++blockId) {
                presignedURIRequestParams.put("blockid",
                        Base64.encode(String.format("%06d", blockId)));
                uploadPartURIs.add(
                        createPresignedURI(key,
                                perms,
                                httpUploadURIExpirySeconds,
                                presignedURIRequestParams,
                                domain)
                );
            }

            try {
                byte[] secret = getOrCreateReferenceKey();
                String uploadToken = new DataRecordUploadToken(blobId, uploadId).getEncodedToken(secret);
                return new DataRecordUpload() {
                    @Override
                    @NotNull
                    public String getUploadToken() {
                        return uploadToken;
                    }

                    @Override
                    public long getMinPartSize() {
                        return minPartSize;
                    }

                    @Override
                    public long getMaxPartSize() {
                        return maxPartSize;
                    }

                    @Override
                    @NotNull
                    public Collection<URI> getUploadURIs() {
                        return uploadPartURIs;
                    }
                };
            } catch (DataStoreException e) {
                LOG.warn("Unable to obtain data store key");
            }
        }

        return null;
    }

    private Long getUncommittedBlocksListSize(BlockBlobClient client) throws DataStoreException {
        List<Block> blocks = client.listBlocks(BlockListType.UNCOMMITTED).getUncommittedBlocks();
        updateLastModifiedMetadata(client);
        client.commitBlockList(blocks.stream().map(Block::getName).collect(Collectors.toList()));
        long size = 0L;
        for (Block block : blocks) {
            size += block.getSize();
        }
        return size;
    }

    protected DataRecord completeHttpUpload(@NotNull String uploadTokenStr)
            throws DataRecordUploadException, DataStoreException {

        if (Strings.isNullOrEmpty(uploadTokenStr)) {
            throw new IllegalArgumentException("uploadToken required");
        }

        DataRecordUploadToken uploadToken = DataRecordUploadToken.fromEncodedToken(uploadTokenStr, getOrCreateReferenceKey());
        String key = uploadToken.getBlobId();
        DataIdentifier blobId = new DataIdentifier(getIdentifierName(key));

        DataRecord record = null;
        try {
            record = getRecord(blobId);
            // If this succeeds this means either it was a "single put" upload
            // (we don't need to do anything in this case - blob is already uploaded)
            // or it was completed before with the same token.
        } catch (DataStoreException e1) {
            // record doesn't exist - so this means we are safe to do the complete request
            try {
                if (uploadToken.getUploadId().isPresent()) {
                    BlockBlobClient blockBlobClient = getAzureContainer().getBlobClient(key).getBlockBlobClient();
                    long size = getUncommittedBlocksListSize(blockBlobClient);
                    record = new AzureBlobStoreDataRecord(
                            this,
                            azureBlobContainerProvider,
                            blobId,
                            getLastModified(blockBlobClient),
                            size);
                } else {
                    // Something is wrong - upload ID missing from upload token
                    // but record doesn't exist already, so this is invalid
                    throw new DataRecordUploadException(
                            String.format("Unable to finalize direct write of binary %s - upload ID missing from upload token",
                                    blobId)
                    );
                }
            } catch (BlobStorageException e2) {
                throw new DataRecordUploadException(
                        String.format("Unable to finalize direct write of binary %s", blobId),
                        e2
                );
            }
        }

        return record;
    }

    private String getDefaultBlobStorageDomain() {
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");
        if (Strings.isNullOrEmpty(accountName)) {
            LOG.warn("Can't generate presigned URI - Azure account name not found in properties");
            return null;
        }
        return String.format("%s.blob.core.windows.net", accountName);
    }

    private String getDirectDownloadBlobStorageDomain(boolean ignoreDomainOverride) {
        String domain = ignoreDomainOverride
                ? getDefaultBlobStorageDomain()
                : downloadDomainOverride;
        if (Strings.isNullOrEmpty(domain)) {
            domain = getDefaultBlobStorageDomain();
        }
        return domain;
    }

    private String getDirectUploadBlobStorageDomain(boolean ignoreDomainOverride) {
        String domain = ignoreDomainOverride
                ? getDefaultBlobStorageDomain()
                : uploadDomainOverride;
        if (Strings.isNullOrEmpty(domain)) {
            domain = getDefaultBlobStorageDomain();
        }
        return domain;
    }

    private URI createPresignedURI(String key,
                                   BlobSasPermission blobSasPermissions,
                                   int expirySeconds,
                                   String domain) {
        return createPresignedURI(key, blobSasPermissions, expirySeconds, Maps.newHashMap(), domain);
    }

    private URI createPresignedURI(String key,
                                   BlobSasPermission blobSasPermissions,
                                   int expirySeconds,
                                   Map<String, String> additionalQueryParams,
                                   String domain) {
        if (Strings.isNullOrEmpty(domain)) {
            LOG.warn("Can't generate presigned URI - no Azure domain provided (is Azure account name configured?)");
            return null;
        }

        URI presignedURI = null;
        try {
            String sharedAccessSignature = azureBlobContainerProvider.generateSharedAccessSignature(retryOptions, key,
                    blobSasPermissions, expirySeconds, properties);

            // Shared access signature is returned encoded already.
            String uriString = String.format("https://%s/%s/%s?%s",
                    domain,
                    getContainerName(),
                    key,
                    sharedAccessSignature);

            if (!additionalQueryParams.isEmpty()) {
                StringBuilder builder = new StringBuilder();
                for (Map.Entry<String, String> e : additionalQueryParams.entrySet()) {
                    builder.append("&");
                    builder.append(URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8));
                    builder.append("=");
                    builder.append(URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8));
                }
                uriString += builder.toString();
            }

            presignedURI = new URI(uriString);
        } catch (DataStoreException e) {
            LOG.error("No connection to Azure Blob Storage", e);
        } catch (URISyntaxException | InvalidKeyException e) {
            LOG.error("Can't generate a presigned URI for key {}", key, e);
        } catch (BlobStorageException e) {
            LOG.error("Azure request to create presigned Azure Blob Storage {} URI failed. " +
                            "Key: {}, Error: {}, HTTP Code: {}, Azure Error Code: {}",
                    blobSasPermissions.hasReadPermission() ? "GET" :
                            ((blobSasPermissions.hasWritePermission()) ? "PUT" : ""),
                    key,
                    e.getMessage(),
                    e.getStatusCode(),
                    e.getErrorCode());
        }

        return presignedURI;
    }

    private static class AzureBlobInfo {
        private final String name;
        private final long lastModified;
        private final long length;

        public AzureBlobInfo(String name, long lastModified, long length) {
            this.name = name;
            this.lastModified = lastModified;
            this.length = length;
        }

        public String getName() {
            return name;
        }

        public long getLastModified() {
            return lastModified;
        }

        public long getLength() {
            return length;
        }

        public static AzureBlobInfo fromCloudBlob(BlockBlobClient cloudBlob) throws BlobStorageException {
            return new AzureBlobInfo(cloudBlob.getBlobName(),
                                     AzureBlobStoreBackend.getLastModified(cloudBlob),
                                     cloudBlob.getProperties().getBlobSize());
        }
    }

    private class RecordsIterator<T> extends AbstractIterator<T> {
        private boolean firstCall = true;
        private final Function<AzureBlobInfo, T> transformer;
        private final Queue<AzureBlobInfo> items = Lists.newLinkedList();
        private final BlobContainerClient containerClient;

        public RecordsIterator(Function<AzureBlobInfo, T> transformer, BlobContainerClient containerClient) {
            this.transformer = transformer;
            this.containerClient = containerClient;
        }

        @Nullable
        @Override
        protected T computeNext() {
            if (items.isEmpty()) {
                loadItems();
            }
            if (!items.isEmpty()) {
                return transformer.apply(items.remove());
            }
            return endOfData();
        }

        private boolean loadItems() {
            long start = System.currentTimeMillis();
            ClassLoader contextClassLoader = currentThread().getContextClassLoader();
            try {
                currentThread().setContextClassLoader(getClass().getClassLoader());

                if (!firstCall) {
                    LOG.trace("No more records in container. containerName={}", containerClient.getBlobContainerName());
                    return false;
                }

                firstCall = false;
                containerClient.listBlobs(new ListBlobsOptions(), null)
                        .forEach(blobItem -> items.add(AzureBlobInfo.fromCloudBlob(containerClient.getBlobClient(blobItem.getName()).getBlockBlobClient())));
                LOG.debug("Container records batch read. batchSize={} containerName={} duration={}",
                        items.size(), getContainerName(), (System.currentTimeMillis() - start));
                return !items.isEmpty();
            } finally {
                if (contextClassLoader != null) {
                    currentThread().setContextClassLoader(contextClassLoader);
                }
            }
        }
    }

    static class AzureBlobStoreDataRecord extends AbstractDataRecord {
        final AzureBlobContainerProvider azureBlobContainerProvider;
        final long lastModified;
        final long length;
        final boolean isMeta;

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, AzureBlobContainerProvider azureBlobContainerProvider,
                                        DataIdentifier key, long lastModified, long length) {
            this(backend, azureBlobContainerProvider, key, lastModified, length, false);
        }

        public AzureBlobStoreDataRecord(AbstractSharedBackend backend, AzureBlobContainerProvider azureBlobContainerProvider,
                                        DataIdentifier key, long lastModified, long length, boolean isMeta) {
            super(backend, key);
            this.azureBlobContainerProvider = azureBlobContainerProvider;
            this.lastModified = lastModified;
            this.length = length;
            this.isMeta = isMeta;
        }

        @Override
        public long getLength() throws DataStoreException {
            return length;
        }

        @Override
        public InputStream getStream() throws DataStoreException {
            String id = getKeyName(getIdentifier());
            BlobContainerClient container = azureBlobContainerProvider.getBlobContainer();
            if (isMeta) {
                id = addMetaKeyPrefix(getIdentifier().toString());
            }
            else {
                // Don't worry about stream logging for metadata records
                if (LOG_STREAMS_DOWNLOAD.isDebugEnabled()) {
                    // Log message, with exception, so we can get a trace to see where the call came from
                    LOG_STREAMS_DOWNLOAD.debug("Binary downloaded from Azure Blob Storage - identifier={} ", id, new Exception());
                }
            }
            try {
                return container.getBlobClient(id).openInputStream();
            } catch (Exception e) {
                throw new DataStoreException(e);
            }
        }

        @Override
        public long getLastModified() {
            return lastModified;
        }

        @Override
        public String toString() {
            return "AzureBlobStoreDataRecord{" +
                   "identifier=" + getIdentifier() +
                   ", length=" + length +
                   ", lastModified=" + lastModified +
                   ", containerName='" + Optional.ofNullable(azureBlobContainerProvider).map(AzureBlobContainerProvider::getContainerName).orElse(null) + '\'' +
                   '}';
        }
    }

    private String getContainerName() {
        return Optional.ofNullable(this.azureBlobContainerProvider)
                .map(AzureBlobContainerProvider::getContainerName)
                .orElse(null);
    }

    @Override
    public byte[] getOrCreateReferenceKey() throws DataStoreException {
        try {
            if (secret != null && secret.length != 0) {
                return secret;
            } else {
                byte[] key;
                // Try reading from the metadata folder if it exists
                key = readMetadataBytes(REF_KEY);
                if (key == null) {
                    key = super.getOrCreateReferenceKey();
                    addMetadataRecord(new ByteArrayInputStream(key), REF_KEY);
                    key = readMetadataBytes(REF_KEY);
                }
                secret = key;
                return secret;
            }
        } catch (IOException e) {
            throw new DataStoreException("Unable to get or create key " + e);
        }
    }

    protected byte[] readMetadataBytes(String name) throws IOException, DataStoreException {
        DataRecord rec = getMetadataRecord(name);
        byte[] key = null;
        if (rec != null) {
            InputStream stream = null;
            try {
                stream = rec.getStream();
                return IOUtils.toByteArray(stream);
            } finally {
                IOUtils.closeQuietly(stream);
            }
        }
        return key;
    }

    private String computeSecondaryLocationEndpoint() {
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");

        boolean enableSecondaryLocation = PropertiesUtil.toBoolean(properties.getProperty(AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME),
                AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT);

        if(enableSecondaryLocation) {
            return String.format("https://%s-secondary.blob.core.windows.net", accountName);
        }

        return null;
    }
}
