package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.util.Properties;


public abstract class AbstractAzureBlobStoreBackend extends AbstractSharedBackend {

    protected abstract DataRecordUpload initiateHttpUpload(long maxUploadSizeInBytes, int maxNumberOfURIs, @NotNull final DataRecordUploadOptions options);
    protected abstract DataRecord completeHttpUpload(@NotNull String uploadTokenStr) throws DataRecordUploadException, DataStoreException;
    protected abstract void setHttpDownloadURIExpirySeconds(int seconds);
    protected abstract  void setHttpUploadURIExpirySeconds(int seconds);
    protected abstract void setHttpDownloadURICacheSize(int maxSize);
    protected abstract URI createHttpDownloadURI(@NotNull DataIdentifier identifier, @NotNull DataRecordDownloadOptions downloadOptions);
    public abstract void setProperties(final Properties properties);

}
