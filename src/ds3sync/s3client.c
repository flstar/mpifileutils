/**
 * @file s3client.c
 *
 * @author - Feng, Lei
 *
 *
 */
#define _GNU_SOURCE

#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <sys/mman.h>
#include <openssl/md5.h>
#include <openssl/evp.h>

#include "s3client.h"
#include "mfu.h"
#include "strmap.h"


#define S3STATUS_BASE       10000
#define MAX_ERROR_MSG_LEN   1024

#define HTTP_SCHEME         "http://"
#define HTTPS_SCHEME        "https://"

#define MAX_ETAG_SIZE      40


static pthread_mutex_t libs3_init_mutex = PTHREAD_MUTEX_INITIALIZER;
static int libs3_init_count             = 0;


/* convert S3Status to errno
 * S3StatusOK is 0, others are 10000 + status
 */
static int from_s3status(S3Status status)
{
    if (status == S3StatusOK) {
        return 0;
    }
    return (S3STATUS_BASE + status);
}

const char *errno2str(int err)
{
    static __thread char errmsg[MAX_ERROR_MSG_LEN];

    if (err < S3STATUS_BASE) {
        snprintf(errmsg, MAX_ERROR_MSG_LEN, "%s", strerror(err));
    }
    else {
        snprintf(errmsg, MAX_ERROR_MSG_LEN, "(libs3)%s",
            S3_get_status_name(err - S3STATUS_BASE));
    }

    return errmsg;
}

static int init_libs3_once(const char *hostport)
{
    S3Status status = S3StatusOK;

    pthread_mutex_lock(&libs3_init_mutex);
    if (libs3_init_count == 0) {
        status = S3_initialize("s3", S3_INIT_ALL, hostport);
        if (status != S3StatusOK) {
            goto out;
        }
    }

    libs3_init_count++;

out:
    pthread_mutex_unlock(&libs3_init_mutex);
    return -from_s3status(status);
}

static void deinit_libs3_once()
{
    pthread_mutex_lock(&libs3_init_mutex);
    if ( (--libs3_init_count) == 0) {
        S3_deinitialize();
    }
    pthread_mutex_unlock(&libs3_init_mutex);
}

s3client_t * s3client_new(
    const char *endpoint,
    const char *bucket,
    const char *accessKey,
    const char *secretKey
)
{
    S3Status status;
    s3client_t *client = NULL;
    S3Protocol prot = S3ProtocolHTTPS;
    const char *hostport = NULL;

    /* parse endpoint to get protocol, hostname */
    if (strncmp(HTTPS_SCHEME, endpoint, strlen(HTTPS_SCHEME)) == 0) {
        prot = S3ProtocolHTTPS;
        hostport = endpoint + strlen(HTTPS_SCHEME);
    }
    else if (strncmp(HTTP_SCHEME, endpoint, strlen(HTTP_SCHEME)) == 0) {
        prot = S3ProtocolHTTP;
        hostport = endpoint + strlen(HTTP_SCHEME);
    }
    else {
        MFU_LOG(MFU_LOG_ERR, "S3 endpoint must start with '%s' or '%s'",
            HTTP_SCHEME, HTTPS_SCHEME);
        errno = EINVAL;
        goto out;
    }

    status = init_libs3_once(hostport);
    if (status) {
        errno = from_s3status(status);
        goto out;
    }

    client = MFU_MALLOC(sizeof(s3client_t));
    memset(client, 0x00, sizeof(s3client_t));

    client->bucketContent.protocol          = prot;
    client->bucketContent.hostName          = MFU_STRDUP(hostport);
    client->bucketContent.bucketName        = MFU_STRDUP(bucket);
    client->bucketContent.accessKeyId       = MFU_STRDUP(accessKey);
    client->bucketContent.secretAccessKey   = MFU_STRDUP(secretKey);
    client->bucketContent.uriStyle          = S3UriStylePath;

    client->list_max_keys = 1000;
    client->try_times = 3;
    client->put_times = 2;

out:
    return client;
}

void s3client_destroy(s3client_t *client)
{
    if (client == NULL) return;

    mfu_free(&client->bucketContent.hostName);
    mfu_free(&client->bucketContent.bucketName);
    mfu_free(&client->bucketContent.accessKeyId);
    mfu_free(&client->bucketContent.secretAccessKey);
    mfu_free(&client->bucketContent.securityToken);
    mfu_free(&client);

    deinit_libs3_once();
}

typedef struct {
    S3Status status;
    char etag[MAX_ETAG_SIZE];
    int64_t mtime;
    uint64_t length;
    strmap *mds;      // to save user metadata
} common_callback_data_t;

static S3Status default_response_properties_callback(
    const S3ResponseProperties *properties,
    void *callbackData)
{
    common_callback_data_t *cb_data = (common_callback_data_t *)callbackData;

    cb_data->mtime = properties->lastModified;
    cb_data->length = properties->contentLength;
    if (properties->eTag != NULL && strlen(properties->eTag) < MAX_ETAG_SIZE) {
        strcpy(cb_data->etag, properties->eTag);
    }

    if (cb_data->mds != NULL) {
        for (int i=0; i<properties->metaDataCount; i++) {
            const S3NameValue *nv = properties->metaData + i;
            strmap_set(cb_data->mds, nv->name, nv->value);
        }
    }

    return S3StatusOK;
}

static void default_response_complete_callback(
    S3Status status,
    const S3ErrorDetails *error,
    void *callbackData)
{
    common_callback_data_t *cb_data = (common_callback_data_t *)(callbackData);
    cb_data->status = status;

    return;
}

static S3ResponseHandler default_response_handler = {
    &default_response_properties_callback,
    &default_response_complete_callback
};

#define RETRY_S3_REQUEST(action, times, status) do { \
    for (int i = 1; i <= times; i++) { \
        action; \
        if (!S3_status_is_retryable(status)) \
            break; \
        int rc = from_s3status(status); \
        MFU_LOG(MFU_LOG_VERBOSE, \
                "S3 request failed for retryable reason after trying for %d times. %d:%s\n", \
                i, rc, errno2str(rc) ); \
    } \
} while (0)

static ssize_t write_full(int fd, const void *vptr, size_t n)
{
    size_t      nleft;
    ssize_t     nwritten;
    const char  *ptr;

    ptr = vptr;
    nleft = n;
    while (nleft > 0) {
        if ( (nwritten = write(fd, ptr, nleft)) <= 0) {
            if (nwritten < 0 && errno == EINTR)
                nwritten = 0;       /* and call write() again */
            else
                return(-1);         /* error */
        }

        nleft -= nwritten;
        ptr   += nwritten;
    }
    return(n);
}

static int stat_to_user_metadata(struct stat *statbuf, strmap *mds)
{
    int rc = 0;
    char buff[64];

    sprintf(buff, "%d", statbuf->st_uid);
    strmap_set(mds, DS3SYNC_MD_OWNER, buff);
    sprintf(buff, "%d", statbuf->st_gid);
    strmap_set(mds, DS3SYNC_MD_GROUP, buff);
    sprintf(buff, "0%o", statbuf->st_mode);
    strmap_set(mds, DS3SYNC_MD_MODE, buff);
    sprintf(buff, "%d.%09d", statbuf->st_mtim.tv_sec, statbuf->st_mtim.tv_nsec);
    strmap_set(mds, DS3SYNC_MD_MTIME, buff);
    sprintf(buff, "%d.%09d", statbuf->st_atim.tv_sec, statbuf->st_atim.tv_nsec);
    strmap_set(mds, DS3SYNC_MD_ATIME, buff);

    return 0;
}

static int user_metadata_to_stat(strmap *mds, struct stat *statbuf)
{
    int rc = 0;
    const strmap_node *node = NULL;

    strmap_foreach(mds, node) {
        if (strcasecmp(DS3SYNC_MD_OWNER, node->key) == 0) {
            rc = sscanf(node->value, "%d", &statbuf->st_uid);
        }
        else if (strcasecmp(DS3SYNC_MD_GROUP, node->key) == 0) {
            rc = sscanf(node->value, "%d", &statbuf->st_gid);
        }
        else if (strcasecmp(DS3SYNC_MD_MODE, node->key) == 0) {
            rc = sscanf(node->value, "0%o", &statbuf->st_mode);
        }
        else if (strcasecmp(DS3SYNC_MD_MTIME, node->key) == 0) {
            rc = sscanf(node->value, "%d.%d", &statbuf->st_mtim.tv_sec, &statbuf->st_mtim.tv_nsec);
        }
        else if (strcasecmp(DS3SYNC_MD_ATIME, node->key) == 0) {
            rc = sscanf(node->value, "%d.%d", &statbuf->st_atim.tv_sec, &statbuf->st_atim.tv_nsec);
        }

        if (rc <= 0) {
            MFU_LOG(MFU_LOG_WARN, "unrecognized format of user-metadata %s=%s",
                node->key, node->value);
        }
    }

    return 0;
}

static int restore_stat(const char *fn, struct stat *statbuf)
{
    int rc = 0;

    /* restore permission */
    mode_t perm = statbuf->st_mode & (S_IRWXU | S_IRWXG | S_IRWXO);
    rc = chmod(fn, perm);
    if (rc) {
        rc = -errno;
        MFU_LOG(MFU_LOG_ERR, "failed to set permission to 0%o for file '%s'. %d:%s",
            perm, fn, -rc, errno2str(-rc));
        goto out;
    }
    /* restore mtime based object's mtime */
    struct timespec times[] = {statbuf->st_atim, statbuf->st_mtim};
    rc = utimensat(AT_FDCWD, fn, times, 0);
    if (rc) {
        rc = -errno;
        MFU_LOG(MFU_LOG_ERR, "failed to update atime/mtime for file '%s'. %d:%s",
            fn, -rc, errno2str(-rc));
        goto out;
    }
    /* restore uid and gid */
    rc = chown(fn, statbuf->st_uid, statbuf->st_gid);
    if (rc) {
        rc = -errno;
        MFU_LOG(MFU_LOG_ERR, "failed to set uid:gid to %d:%d for file '%s'. %d.%s",
            statbuf->st_uid, statbuf->st_gid, fn, -rc, errno2str(-rc));
        goto out;
    }

out:
    return rc;
}

typedef struct {
    common_callback_data_t common;
    int fd;
} get_file_callback_data_t;

static S3Status _get_file_callback(int bufferSize, const char *buffer, void *callbackData)
{
    get_file_callback_data_t *cb_data = (get_file_callback_data_t *)callbackData;

    int rc = write_full(cb_data->fd, buffer, bufferSize);
    if (rc != bufferSize) {
        return -errno;
    }
    else {
        return S3StatusOK;
    }
}

int s3client_get_file(s3client_t *client, const char *key, const char *fn)
{
    int rc = -from_s3status(S3StatusInternalError);
    int fd = -1;
    get_file_callback_data_t cb_data;
    struct stat stat1;

    memset(&cb_data, 0x00, sizeof(cb_data));
    cb_data.common.mds = strmap_new();

    fd = open(fn, O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        rc = -errno;
        goto out;
    }
    cb_data.fd = fd;

    S3GetObjectHandler handler = {
       default_response_handler,
       &_get_file_callback
    };

    RETRY_S3_REQUEST(
        do {
            lseek(fd, 0, SEEK_SET);
            S3_get_object(&client->bucketContent, key, NULL, 0, 0, NULL, 0, &handler, &cb_data);
        } while (0),
        client->try_times,
        cb_data.common.status
    );

    if (cb_data.common.status != S3StatusOK) {
        rc = -from_s3status(cb_data.common.status);
        MFU_LOG(MFU_LOG_ERR, "failed to get file '%s' from object '%s'. %d:%s",
            fn, key, -rc, errno2str(-rc));

        goto out;
    }

    /* restore stat */
    memset(&stat1, 0x00, sizeof(stat1));
    stat1.st_size = cb_data.common.length;
    stat1.st_uid = getuid();
    stat1.st_gid = getgid();
    stat1.st_mtim.tv_sec = cb_data.common.mtime;
    stat1.st_atim.tv_nsec = UTIME_OMIT;

    user_metadata_to_stat(cb_data.common.mds, &stat1);

    /* truncate file to exactly the length of object */
    rc = ftruncate(fd, stat1.st_size);
    if (rc) {
        rc = -errno;
        MFU_LOG(MFU_LOG_ERR, "failed to truncate file '%s' to length %llu. %d:%s",
            fn, cb_data.common.length, -rc, errno2str(-rc));
        goto out;
    }

    close(fd);
    fd = -1;

    rc = restore_stat(fn, &stat1);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to restore file stat. %d:%s",
            -rc, errno2str(-rc));
        goto out;
    }

out:
    if (fd >= 0) {
        close(fd);
    }
    strmap_delete(&cb_data.common.mds);
    return rc;
}

typedef struct {
    common_callback_data_t common;

    int prefix_len;
    int is_truncated;
    char *marker;

    strmap *entries;
} list_tree_callback_data_t;

static S3Status _list_tree_callback(
    int isTruncated,
    const char *nextMarker,
    int contentsCount,
    const S3ListBucketContent *contents,
    int commonPrefixesCount,
    const char **commonPrefixes,
    void *callbackData)
{
    list_tree_callback_data_t *cb_data = (list_tree_callback_data_t *)callbackData;

    cb_data->is_truncated = isTruncated;
    mfu_free(&cb_data->marker);
    cb_data->marker = MFU_STRDUP(nextMarker);

    for (int i = 0; i < contentsCount; i++) {
        strmap_set(cb_data->entries, contents[i].key + cb_data->prefix_len, "");
    }

    return S3StatusOK;
}

int s3client_list_tree(s3client_t *client, const char *path, strmap *entries)
{
    int rc = -from_s3status(S3StatusInternalError);
    list_tree_callback_data_t cb_data;
    char *prefix = MFU_MALLOC(strlen(path) + 2);

    sprintf(prefix, "%s/", path);

    memset(&cb_data, 0x00, sizeof(cb_data));
    cb_data.prefix_len = strlen(prefix);
    cb_data.entries = entries;

    S3ListBucketHandler handler = {
        default_response_handler,
        &_list_tree_callback
    };

    do {
        RETRY_S3_REQUEST(
            S3_list_bucket(&client->bucketContent, prefix, cb_data.marker, NULL,
                client->list_max_keys, NULL, 0, &handler, &cb_data),
            client->try_times,
            cb_data.common.status
        );
    } while (cb_data.common.status == S3StatusOK && cb_data.is_truncated);

    rc = -from_s3status(cb_data.common.status);

out:
    mfu_free(&prefix);
    return rc;
}

/* head s3 object and fill some fields in statbuf
 *   - for an existing object, fill st_mode as S_IFREG, mtime and size
 *   - for non-existing object, fill st_mode as S_IFDIR
 */
int s3client_stat_path(s3client_t *client, const char *key, struct stat *statbuf)
{
    int rc = 0;
    common_callback_data_t cb_data;

    memset(&cb_data, 0x00, sizeof(cb_data));
    cb_data.status = S3StatusInternalError;
    cb_data.mds = strmap_new();

    RETRY_S3_REQUEST(
        S3_head_object(&client->bucketContent, key, NULL, 0, &default_response_handler, &cb_data),
        client->try_times,
        cb_data.status
    );

    if (cb_data.status == S3StatusOK) {
        statbuf->st_mode = S_IFREG;
        statbuf->st_size = cb_data.length;
        statbuf->st_mtim.tv_sec = cb_data.mtime;
        statbuf->st_mtim.tv_nsec = 0;
        statbuf->st_atim.tv_nsec = UTIME_OMIT;
        user_metadata_to_stat(cb_data.mds, statbuf);

        rc = 0;
    }
    else if (cb_data.status == S3StatusHttpErrorNotFound ||
             cb_data.status == S3StatusErrorNoSuchKey)
    {
        /* if object does not exist, test it as a directory */
        strmap *entries = strmap_new();
        rc = s3client_list_tree(client, key, entries);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "failed to list object under '%s'. %d:%s",
                key, -rc, errno2str(-rc));
        }
        else if (strmap_size(entries) == 0) {
            statbuf->st_mode = 0;
            rc = -ENOENT;
        }
        else {
            statbuf->st_mode = S_IFDIR;
        }
        strmap_delete(&entries);
    }
    else {
        rc = -from_s3status(cb_data.status);
    }

    strmap_delete(&cb_data.mds);
    return rc;
}

int s3client_test_object(s3client_t *client, const char *key, const char *etag)
{
    int rc = -from_s3status(S3StatusInternalError), i;
    common_callback_data_t cb_data;
    memset(&cb_data, 0x00, sizeof(cb_data));
    cb_data.status = S3StatusInternalError;

    for (i = 0; i < client->try_times; i++) {
        S3_head_object(&client->bucketContent, key, NULL, 0, &default_response_handler, &cb_data);
        if (S3_status_is_retryable(cb_data.status)) {
            continue;
        }
        break;
    }

    rc = -from_s3status(cb_data.status);
    if (rc) {
        goto out;
    }

    if (etag != NULL && strcmp(etag, cb_data.etag) != 0) {
        rc = -from_s3status(S3StatusBadIfMatchETag);
    }

out:
    return rc;
}

static int md5_file(int fd, unsigned char *result)
{
    int rc = 0;
    struct stat stat1;
    char *data = NULL;

    rc = fstat(fd, &stat1);
    if (rc) {
        rc = -errno;
        goto out;
    }

    data = mmap(0, stat1.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (data == NULL) {
        rc = -errno;
        goto out;
    }

    MD5(data, stat1.st_size, result);
    rc = 0;

out:
    if (data != NULL) {
        munmap(data, stat1.st_size);
    }
    return rc;
}

typedef struct {
    common_callback_data_t common;
    int fd;
} put_file_callback_data_t;

static int _put_file_callback(int bufferSize, char *buffer, void *callbackData)
{
    put_file_callback_data_t *cb_data = (put_file_callback_data_t *)callbackData;
    int rc = read(cb_data->fd, buffer, bufferSize);

    return rc;
}

static int s3client_put_file_once(s3client_t *client, const char *key, const char *fn)
{
    int rc = -from_s3status(S3StatusInternalError);
    int fd = -1, i, md_num;
    put_file_callback_data_t cb_data;
    unsigned char md5[MD5_DIGEST_LENGTH];
    char b64md5[2 * MD5_DIGEST_LENGTH];
    char etag[MAX_ETAG_SIZE];
    struct stat stat1;
    S3NameValue nvs[DS3SYNC_MAX_MD_NUM];
    const strmap_node *node = NULL;

    memset(&cb_data, 0x00, sizeof(cb_data));
    cb_data.common.status = S3StatusInternalError;
    cb_data.common.mds = strmap_new();

    // open local file
    fd = open(fn, O_RDONLY | O_NOATIME);
    if (fd < 0) {
        rc = -errno;
        goto out;
    }
    cb_data.fd = fd;

    // calculate md5 checksum, generate base64-md5 and etag
    rc = md5_file(fd, md5);
    if (rc != 0) {
        goto out;
    }

    EVP_EncodeBlock(b64md5, md5, MD5_DIGEST_LENGTH);

    etag[0] = '"';
    for (i = 0; i < MD5_DIGEST_LENGTH; i++) {
        sprintf(etag + 2 * i + 1, "%02x", (unsigned char)md5[i]);
    }
    etag[2 * MD5_DIGEST_LENGTH + 1] = '"';
    etag[2 * MD5_DIGEST_LENGTH + 2] = '\0';

    memset(&stat1, 0x00, sizeof(stat1));
    rc = fstat(fd, &stat1);
    if (rc != 0) {
        rc = -errno;
        goto out;
    }

    stat_to_user_metadata(&stat1, cb_data.common.mds);

    md_num = 0;
    strmap_foreach(cb_data.common.mds, node) {
        nvs[md_num].name = node->key;
        nvs[md_num].value = node->value;
        md_num++;
    }

    S3PutObjectHandler handler = {
        default_response_handler,
        &_put_file_callback
    };

    S3PutProperties props = {NULL, b64md5, NULL, NULL, NULL, -1, 0, md_num, nvs, 0};

    RETRY_S3_REQUEST(
        do {
            lseek(fd, 0, SEEK_SET);
            S3_put_object(&client->bucketContent, key, stat1.st_size, &props,
                          NULL, 0, &handler, &cb_data);
        } while(0),
        client->try_times,
        cb_data.common.status
    );

    rc = -from_s3status(cb_data.common.status);
    if (rc != 0) {
        goto out;
    }

    rc = s3client_test_object(client, key, etag);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_VERBOSE, "failed to check existence of object %s. %d:%s",
            key, -rc, errno2str(-rc));
    }

out:
    if (fd >= 0) {
        close(fd);
    }
    strmap_delete(&cb_data.common.mds);
    return rc;
}

int s3client_put_file(s3client_t *client, const char *key, const char *fn)
{
    int rc = -from_s3status(S3StatusInternalError);

    for (int i = 1; i <= client->put_times; i++) {
        rc = s3client_put_file_once(client, key, fn);
        if (rc && (-rc) >= S3STATUS_BASE && i < client->put_times) {
            int secs = 10;
            MFU_LOG(MFU_LOG_VERBOSE, "failed to put object %s, %d:%s. Retry in %d seconds.",
                key, -rc, errno2str(-rc), secs);
            sleep(secs);
        }
        else {
            break;
        }
    }

    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to put object after trying for %d times. %d:%s",
            client->put_times, -rc, errno2str(-rc));
    }

    return rc;
}

int s3client_delete_object(s3client_t * client, const char * key)
{
    int rc = 0;
    common_callback_data_t cb_data;
    memset(&cb_data, 0x00, sizeof(cb_data));
    cb_data.status = S3StatusInternalError;

    RETRY_S3_REQUEST(
        S3_delete_object(&client->bucketContent, key, NULL, 0, &default_response_handler, &cb_data),
        client->try_times,
        cb_data.status
    );

    if (cb_data.status == S3StatusHttpErrorNotFound) {
        rc = -ENOENT;
    }
    else {
        rc = -from_s3status(cb_data.status);
    }

    return rc;
}

