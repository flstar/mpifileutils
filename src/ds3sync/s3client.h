#ifndef __S3CLIENT_H__
#define __S3CLIENT_H__

#include <stdbool.h>
#include <libs3.h>

#include "strmap.h"

typedef struct {
    S3BucketContext bucketContent;
    int list_max_keys;
    int try_times;      // how many times to try if request fails for retryable reason
    int put_times;      // how many times to put an object if it fails to be tested
} s3client_t;

extern const char *errno2str(int err);

extern s3client_t * s3client_new(
    const char *endpoint,
    const char *bucket,
    const char *accessKey,
    const char *secretKey
);

extern void s3client_destroy(s3client_t *client);

extern int s3client_get_file(s3client_t *client, const char *key, const char *fn);

extern int s3client_list_tree(s3client_t *client, const char *prefix, strmap *entries);

extern int s3client_stat_path(s3client_t *client, const char *key, struct stat *statbuf);

extern int s3client_test_object(s3client_t *client, const char *key, const char *etag);

extern int s3client_put_file(s3client_t *client, const char *key, const char *fn);

#endif

