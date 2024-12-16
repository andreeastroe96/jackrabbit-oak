package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

public interface Constants {
    String META_DIR_NAME = "META";
    String META_KEY_PREFIX = META_DIR_NAME + "/";

    String REF_KEY = "reference.key";
    String LAST_MODIFIED_KEY = "lastModified";

    long BUFFERED_STREAM_THRESHOLD = 1024 * 1024;
    long MIN_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 10; // 10MB
    long MAX_MULTIPART_UPLOAD_PART_SIZE = 1024 * 1024 * 100; // 100MB
    long MAX_SINGLE_PUT_UPLOAD_SIZE = 1024 * 1024 * 256; // 256MB, Azure limit
    long MAX_BINARY_UPLOAD_SIZE = (long) Math.floor(1024L * 1024L * 1024L * 1024L * 4.75); // 4.75TB, Azure limit
    int MAX_ALLOWABLE_UPLOAD_URIS = 50000; // Azure limit
    int MAX_UNIQUE_RECORD_TRIES = 10;
    int DEFAULT_CONCURRENT_REQUEST_COUNT = 2;
    int MAX_CONCURRENT_REQUEST_COUNT = 50;
}
