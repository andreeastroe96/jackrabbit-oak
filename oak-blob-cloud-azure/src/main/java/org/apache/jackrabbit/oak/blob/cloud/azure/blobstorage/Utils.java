package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Properties;

public class Utils {
    public static final String DASH = "-";
    public static final String DEFAULT_CONFIG_FILE = "azure.properties";

    public Utils() {}

    public static BlobContainerClient getBlobContainer(@NotNull final String connectionString,
                                                       @NotNull final String containerName,
                                                       @Nullable final RequestRetryOptions retryOptions,
                                                       final Properties properties) throws DataStoreException {
        try {
            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .retryOptions(retryOptions);

                HttpClient httpClient = new NettyAsyncHttpClientBuilder()
                        .proxy(computeProxyOptions(properties))
                        .build();

                builder.httpClient(httpClient);

            BlobServiceClient blobServiceClient = builder.buildClient();
            return blobServiceClient.getBlobContainerClient(containerName);

        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    public static ProxyOptions computeProxyOptions(final Properties properties) {
        String proxyHost = properties.getProperty(AzureConstants.PROXY_HOST);
        String proxyPort = properties.getProperty(AzureConstants.PROXY_PORT);

        if(!Strings.isNullOrEmpty(proxyHost) && Strings.isNullOrEmpty(proxyPort)) {
            return new ProxyOptions(ProxyOptions.Type.HTTP,
                    new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }
        return null;
    }

    public static RequestRetryOptions getRetryOptions(final String maxRequestRetryCount, Integer requestTimeout, String secondaryLocation) {
        int retries = PropertiesUtil.toInteger(maxRequestRetryCount, -1);
        if(retries < 0) {
            return null;
        }

        if (retries == 0) {
            return new RequestRetryOptions(RetryPolicyType.FIXED, 1,
                    requestTimeout, null, null,
                    secondaryLocation);
        }
        return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, retries,
                requestTimeout, null, null,
                secondaryLocation);
    }

    public static String getConnectionStringFromProperties(Properties properties) {
        String sasUri = properties.getProperty(AzureConstants.AZURE_SAS, "");
        String blobEndpoint = properties.getProperty(AzureConstants.AZURE_BLOB_ENDPOINT, "");
        String connectionString = properties.getProperty(AzureConstants.AZURE_CONNECTION_STRING, "");
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");
        String accountKey = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "");

        if (!connectionString.isEmpty()) {
            return connectionString;
        }

        if (!sasUri.isEmpty()) {
            return getConnectionStringForSas(sasUri, blobEndpoint, accountName);
        }

        return getConnectionString(
                accountName,
                accountKey,
                blobEndpoint);
    }

    public static String getConnectionStringForSas(String sasUri, String blobEndpoint, String accountName) {
        if (StringUtils.isEmpty(blobEndpoint)) {
            return String.format("AccountName=%s;SharedAccessSignature=%s", accountName, sasUri);
        } else {
            return String.format("BlobEndpoint=%s;SharedAccessSignature=%s", blobEndpoint, sasUri);
        }
    }

    public static String getConnectionString(final String accountName, final String accountKey, String blobEndpoint) {
        StringBuilder connString = new StringBuilder("DefaultEndpointsProtocol=https");
        connString.append(";AccountName=").append(accountName);
        connString.append(";AccountKey=").append(accountKey);

        if (!Strings.isNullOrEmpty(blobEndpoint)) {
            connString.append(";BlobEndpoint=").append(blobEndpoint);
        }
        return connString.toString();
    }

    public static BlobContainerClient getBlobContainerFromConnectionString(final String azureConnectionString, final String accountName) {
        return new BlobContainerClientBuilder()
                .connectionString(azureConnectionString)
                .containerName(accountName)
                .buildClient();
    }

    /**
     * Read a configuration properties file. If the file name ends with ";burn",
     * the file is deleted after reading.
     *
     * @param fileName the properties file name
     * @return the properties
     * @throws java.io.IOException if the file doesn't exist
     */
    public static Properties readConfig(String fileName) throws IOException {
        if (!new File(fileName).exists()) {
            throw new IOException("Config file not found. fileName=" + fileName);
        }
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(fileName);
            prop.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return prop;
    }
}
