package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class UtilsV8Test {

    @Test
    public void testConnectionStringIsBasedOnProperty() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_CONNECTION_STRING, "DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey");
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,"DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey");
    }

    @Test
    public void testConnectionStringIsBasedOnSAS() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint");
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }

    @Test
    public void testConnectionStringIsBasedOnSASWithoutEndpoint() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "account");
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("AccountName=%s;SharedAccessSignature=%s", "account", "sas"));
    }

    @Test
    public void testConnectionStringIsBasedOnAccessKeyIfSASMissing() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "accessKey");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "secretKey");

        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s","accessKey","secretKey"));
    }

    @Test
    public void testConnectionStringSASIsPriority() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint");

        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "accessKey");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "secretKey");

        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }

}