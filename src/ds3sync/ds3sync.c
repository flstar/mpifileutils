#ifndef _XOPEN_SOURCE
    #define _XOPEN_SOURCE 500
#endif
#ifndef _DEFAULT_SOURCE
    #define _DEFAULT_SOURCE
#endif

#include <stdio.h>
#include <stdbool.h>
#include <errno.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <libgen.h>

#include "mpi.h"
#include "libcircle.h"
#include "mfu.h"
#include "s3client.h"

#define S3_SCHEME                           "s3://"

#define DS3SYNC_OPT_S3_ENDPOINT             1
#define DS3SYNC_OPT_S3_ACCESS_KEY_ID        2
#define DS3SYNC_OPT_S3_SECRET_ACCESS_KEY    3
#define DS3SYNC_OPT_OVERWRITE               4

#define S3_ENDPOINT                         "S3_ENDPOINT"
#define S3_ACCESS_KEY_ID                    "S3_ACCESS_KEY_ID"
#define S3_SECRET_ACCESS_KEY                "S3_SECRET_ACCESS_KEY"

typedef enum {
    DS3SYNC_OPCODE_UPLOAD   = 0,
    DS3SYNC_OPCODE_DOWNLOAD = 1,
} ds3sync_opcode_t;

typedef struct {
    bool help;
    ds3sync_opcode_t opcode;
    char *local;
    char *remote;

    char *s3_endpoint;
    char *s3_access_key_id;
    char *s3_secret_access_key;

    char *s3_bucket;
    char *s3_prefix;

    bool overwrite;
} ds3sync_opts_t;
static ds3sync_opts_t *opts;

s3client_t *s3client = NULL;

static ds3sync_opts_t * ds3sync_opts_new()
{
    ds3sync_opts_t *options = MFU_MALLOC(sizeof(ds3sync_opts_t));
    memset(options, 0x00, sizeof(ds3sync_opts_t));
    return options;
}

static void ds3sync_opts_destroy(ds3sync_opts_t **opts)
{
    mfu_free(&(*opts)->local);
    mfu_free(&(*opts)->remote);
    mfu_free(&(*opts)->s3_endpoint);
    mfu_free(&(*opts)->s3_access_key_id);
    mfu_free(&(*opts)->s3_secret_access_key);
    mfu_free(&(*opts)->s3_bucket);
    mfu_free(&(*opts)->s3_prefix);
    mfu_free(opts);
}

static void ds3sync_opts_dump(ds3sync_opts_t * opts)
{
    MFU_LOG(MFU_LOG_INFO, "ds3sync args:");
    MFU_LOG(MFU_LOG_INFO, "  help   = %s", opts->help ? "true" : "false");
    MFU_LOG(MFU_LOG_INFO, "  opcode = %s", opts->opcode ? "download" : "upload");
    MFU_LOG(MFU_LOG_INFO, "  local  = '%s'", opts->local);
    MFU_LOG(MFU_LOG_INFO, "  remote = '%s'", opts->remote);
    MFU_LOG(MFU_LOG_INFO, "  s3_endpoint          = '%s'", opts->s3_endpoint);
    MFU_LOG(MFU_LOG_INFO, "  s3_access_key_id     = '%s'", opts->s3_access_key_id);
    MFU_LOG(MFU_LOG_INFO, "  s3_secret_access_key = '%s'", opts->s3_secret_access_key);
    MFU_LOG(MFU_LOG_INFO, "  s3_bucket            = '%s'", opts->s3_bucket);
    MFU_LOG(MFU_LOG_INFO, "  s3_prefix            = '%s'", opts->s3_prefix);
    MFU_LOG(MFU_LOG_INFO, "  overwrite            = %s", opts->overwrite ? "true" : "false");
}

static void print_usage(void)
{
    char *str = \
"\n"
"Usage: ds3sync [options] SOURCE TARGET\n"
"\n"
"  SOURCE and TARGET must be one local path and one remote path.\n"
"  Remote path is specified as:\n"
"    s3://<bucket>/<key>\n"
"  For example:\n"
"    s3://bucket-1/testdir/file-1\n"
"\n";
    printf(str);

    str = \
"Options:\n"
"  -h, --help              print usage\n"
"\n";
    printf(str);

    str = \
"  --s3-endpoint=ENDPOINT\n"
"    The endpoint of s3 serivce. For example:\n"
"      http://localhost:9000\n"
"      https://s3.us-west-1.amazonaws.com\n"
"\n"
"    It can also be specified as environment var S3_ENDPOINT. Command line \n"
"    option is of higher priority.\n"
"\n";
    printf(str);

    str = \
"  --s3-access-key-id=ACCESS_KEY_ID\n"
"    The access key id of s3 service. It can also be specified as environment\n"
"    var S3_ACCESS_KEY_ID. Command line option is of higher priority.\n"
"\n";
    printf(str);

    str = \
"  --s3-secret-access-key=SECRET_ACCESS_KEY\n"
"    The secret access key of s3 service. It can also be specified as\n"
"    environment var S3_SECRET_ACCESS_KEY. Command line option is of higher\n"
"    priority.\n"
"\n";
    printf(str);

    str = \
"  --overwrite\n"
"    Overwrite original local file while downloading a file. It's a little\n"
"    dangerous because the downloading may fail and leave a corrupted file.\n"
"\n"
"    The option is false by default. In this case, object is downloaded to\n"
"    a temporary file. Only if the temporary file is downloaded successfully,\n"
"    it will be renamed to the real file.\n"
"\n";
    printf(str);
}

static bool is_s3_scheme(const char *str)
{
    return (strncmp(S3_SCHEME, str, strlen(S3_SCHEME)) == 0);
}

static int ds3sync_parse_args(int argc, char *argv[])
{
    int rc = 0;

    struct option long_options[] = {
        {"help", no_argument, 0, 'h'},
        {"s3-endpoint", required_argument, 0, DS3SYNC_OPT_S3_ENDPOINT},
        {"s3-access-key-id", required_argument, 0, DS3SYNC_OPT_S3_ACCESS_KEY_ID},
        {"s3-secret-access-key", required_argument, 0, DS3SYNC_OPT_S3_SECRET_ACCESS_KEY},
        {"overwrite", no_argument, 0, DS3SYNC_OPT_OVERWRITE},
        {0, 0, 0, 0}
    };

    while (1) {
        int c, option_index;
        c = getopt_long(argc, argv, "h", long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch(c) {
            case 'h':
                opts->help = true;
                goto out;
            case DS3SYNC_OPT_S3_ENDPOINT:
                opts->s3_endpoint = MFU_STRDUP(optarg);
                break;
            case DS3SYNC_OPT_S3_ACCESS_KEY_ID:
                opts->s3_access_key_id = MFU_STRDUP(optarg);
                break;
            case DS3SYNC_OPT_S3_SECRET_ACCESS_KEY:
                opts->s3_secret_access_key = MFU_STRDUP(optarg);
                break;
            case DS3SYNC_OPT_OVERWRITE:
                opts->overwrite = true;
                break;
            default:
                opts->help = true;
                rc = -EINVAL;
                goto out;
        }
    }

    /* now we have two position arguments: source and destination */
    if (argc - optind != 2) {
        MFU_LOG(MFU_LOG_ERR, "you must specify one source and one destination path");
        opts->help = true;
        rc = -EINVAL;
        goto out;
    }

    char *src = argv[optind++];
    char *dst = argv[optind++];

    if (!is_s3_scheme(src)) {
        opts->opcode = DS3SYNC_OPCODE_UPLOAD;
        opts->local = MFU_STRDUP(src);

        if (!is_s3_scheme(dst)) {
            MFU_LOG(MFU_LOG_ERR, "one of source and destination must be local and the other must be remote");
            rc = -EINVAL;
            goto out;
        }
        opts->remote = MFU_STRDUP(dst);
    }
    else {
        opts->opcode = DS3SYNC_OPCODE_DOWNLOAD;
        opts->remote = MFU_STRDUP(src);
        if (is_s3_scheme(dst)) {
            MFU_LOG(MFU_LOG_ERR, "one of source and destination must be local and the other must be remote");
            rc = -EINVAL;
            goto out;
        }
        opts->local = MFU_STRDUP(dst);
    }

out:
    return rc;
}

/**
 * Convert and simplify local path to an absolute path.
 * If isdir is true, verify the path is a directory.
 */
static int verify_local_path(char **path, bool isdir)
{
    int rc = 0;
    char *p, rpath[PATH_MAX];

    /* resolve the path */
    p = realpath(*path, rpath);
    if (p == NULL) {
        rc = -errno;
        MFU_LOG(MFU_LOG_ERR, "failed to resolve local path '%s'. %d:%s",
                *path, -rc, errno2str(-rc));
        goto out;
    }

    if (isdir) {
        /* check local path is a dir */
        struct stat statbuf;
        rc = lstat(rpath, &statbuf);
        if (rc) {
            rc = -errno;
            MFU_LOG(MFU_LOG_ERR, "failed to stat local path '%s'. %d:%s",
                *path, -rc, errno2str(-rc));
            goto out;
        }
        if (!S_ISDIR(statbuf.st_mode)) {
            rc = -ENOTDIR;
            MFU_LOG(MFU_LOG_ERR, "local path '%s' is not a directory", rpath);
            goto out;
        }
    }

    /* local path is OK, replace it with resolved path */
    mfu_free(path);
    *path = MFU_STRDUP(rpath);

out:
    return rc;
}

/* for download operation, the remote path must exists.
 * Or a typo may delete all local data */
static int verify_remote_path()
{
    int rc = 0;
    struct stat stat1;

    memset(&stat1, 0x00, sizeof(stat1));
    rc = s3client_stat_path(s3client, opts->s3_prefix, &stat1);
    if (rc != 0) {
        MFU_LOG(MFU_LOG_ERR, "failed to test remote path s3://%s/%s. %d:%s",
            opts->s3_bucket, opts->s3_prefix, -rc, errno2str(-rc));
        goto out;
    }

out:
    return rc;
}

/* verify and analyze arguments */
static int ds3sync_verify_args()
{
    int rc = 0;

    if (opts->opcode == DS3SYNC_OPCODE_UPLOAD) {
        rc = verify_local_path(&opts->local, false);
        if (rc) {
            goto out;
        }
    }

    if (opts->opcode == DS3SYNC_OPCODE_DOWNLOAD) {
        /* for download, we need to verify the parent dir of local path exists
           and is a directory */
        char tmp[PATH_MAX], *dname, *bname;
        strncpy(tmp, opts->local, PATH_MAX);
        dname = MFU_STRDUP(dirname(tmp));
        rc = verify_local_path(&dname, true);
        if (rc) {
            mfu_free(&dname);
            goto out;
        }

        strncpy(tmp, opts->local, PATH_MAX);
        bname = basename(tmp);

        mfu_free(&opts->local);
        opts->local = MFU_MALLOC(strlen(dname) + strlen(bname) + 2);
        sprintf(opts->local, "%s/%s", dname, bname);
        mfu_free(&dname);
    }

    if (opts->s3_endpoint == NULL) {
        char *p = getenv(S3_ENDPOINT);
        if (p == NULL) {
            MFU_LOG(MFU_LOG_ERR, "S3 endpoint must be provided");
            rc = -EINVAL;
            goto out;
        }
        opts->s3_endpoint = MFU_STRDUP(p);
    }

    if (opts->s3_access_key_id == NULL) {
        char *p = getenv(S3_ACCESS_KEY_ID);
        if (p == NULL) {
            MFU_LOG(MFU_LOG_ERR, "S3 access key id must be provided");
            rc = -EINVAL;
            goto out;
        }
        opts->s3_access_key_id = MFU_STRDUP(p);
    }

    if (opts->s3_secret_access_key == NULL) {
        char *p = getenv(S3_SECRET_ACCESS_KEY);
        if (p == NULL) {
            MFU_LOG(MFU_LOG_ERR, "S3 secret access key must be provided");
            rc = -EINVAL;
            goto out;
        }
        opts->s3_secret_access_key = MFU_STRDUP(p);
    }

    /* check remote path, get bucket and prefix */
    char *p1, *p2;
    p1 = opts->remote + strlen(S3_SCHEME);
    p2 = strchr(p1, '/');
    if (p2 == NULL) {
        MFU_LOG(MFU_LOG_ERR, "invalid remote path '%s'", opts->remote);
        rc = -EINVAL;
        goto out;
    }

    opts->s3_bucket = MFU_MALLOC(p2 - p1 + 1);
    memcpy(opts->s3_bucket, p1, p2 - p1);
    opts->s3_bucket[p2 - p1] = '\0';

    opts->s3_prefix = MFU_STRDUP(p2 + 1);

out:
    return rc;
}

static int compare_timespec(const struct timespec *ts1, const struct timespec *ts2)
{
    if (ts1->tv_sec < ts2->tv_sec) {
        return -1;
    }
    else if (ts1->tv_sec > ts2->tv_sec) {
        return 1;
    }
    /* now sec is the same */
    else if (ts1->tv_nsec < ts2->tv_nsec) {
        return -1;
    }
    else if (ts1->tv_nsec > ts2->tv_nsec) {
        return 1;
    }
    else {
        return 0;
    }
}

static int prepare_dir(const char *path)
{
    int rc = 0;
    char *tmp = MFU_MALLOC(PATH_MAX);
    struct stat stat1;

    memset(&stat1, 0x00, sizeof(stat1));
    rc = stat(path, &stat1);
    if (rc) {
        if (errno != ENOENT) {
            rc = -errno;
            MFU_LOG(MFU_LOG_ERR, "failed to stat path '%s'. %d:%s",
                path, -rc, errno2str(-rc));
            goto out;
        }
        /* if dir does not exist, prepare its parent and create itself*/
        strncpy(tmp, path, PATH_MAX);
        char *parent = dirname(tmp);
        rc = prepare_dir(parent);
        if (rc) {
            goto out;
        }
        rc = mkdir(path, 0755);
        if (rc) {
            rc = -errno;
            MFU_LOG(MFU_LOG_ERR, "failed to create dir '%s'. %d:%s",
                path, -rc, errno2str(-rc));
            goto out;
        }
    }
    else if (!S_ISDIR(stat1.st_mode)) {
        MFU_LOG(MFU_LOG_ERR, "path '%s' is not a dir.", path);
        rc = -ENOTDIR;
    }

out:
    mfu_free(&tmp);
    return rc;
}

static int ds3sync_download_file(const char *key, const char *abspath)
{
    int rc;
    char *tmp_path = MFU_MALLOC(PATH_MAX);
    bool remove_tmp = false;

    /* first prepare parent dir for file */
    strncpy(tmp_path, abspath, PATH_MAX);
    rc = prepare_dir(dirname(tmp_path));
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to prepare directory for file '%s'. %d:%s",
            abspath, -rc, errno2str(-rc));
        goto out;
    }

    if (opts->overwrite) {
        snprintf(tmp_path, PATH_MAX, "%s", abspath);
    }
    else {
        snprintf(tmp_path, PATH_MAX, "%s.tmp.%d", abspath, time(NULL));
        remove_tmp = true;
    }

    /* download object to file */
    rc = s3client_get_file(s3client, key, tmp_path);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to download object 's3://%s/%s' to '%s'. %d:%s",
            opts->s3_bucket, key, tmp_path, -rc, errno2str(-rc));
        goto out;
    }

    if (!opts->overwrite) {
        /* rename tmp file to abspath */
        rc = rename(tmp_path, abspath);
        if (rc) {
            rc = -errno;
            MFU_LOG(MFU_LOG_ERR, "failed to rename '%s' to '%s'. %d:%s",
                tmp_path, abspath, -rc, errno2str(-rc));
            goto out;
        }
    }

out:
    if (remove_tmp) {
        unlink(tmp_path);
    }
    mfu_free(&tmp_path);
    return rc;
}

static int ds3sync_sync_download_entry(const char *entry)
{
    int rc = 0;
    char *abspath = MFU_MALLOC(PATH_MAX);
    char *key = MFU_MALLOC(PATH_MAX);
    struct stat stat1, stat2;

    if (entry[0] == '\0') {
        snprintf(abspath, PATH_MAX, "%s", opts->local);
        snprintf(key, PATH_MAX, "%s", opts->s3_prefix);
    }
    else {
        snprintf(abspath, PATH_MAX, "%s/%s", opts->local, entry);
        snprintf(key, PATH_MAX, "%s/%s", opts->s3_prefix, entry);
    }

    memset(&stat1, 0x00, sizeof(stat1));
    rc = s3client_stat_path(s3client, key, &stat1);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to stat object 's3://%s/%s'. %d:%s",
            opts->s3_bucket, key, -rc, errno2str(-rc));
        goto out;
    }

    memset(&stat2, 0x00, sizeof(stat2));
    rc = stat(abspath, &stat2);
    if (rc && errno != ENOENT) {
        rc = -errno;
        MFU_LOG(MFU_LOG_ERR, "failed to stat path '%s'. %d:%s",
            abspath, -rc, errno2str(-rc));
        goto out;
    }
    rc = 0;

    if (stat2.st_mode == 0 ||
        stat1.st_size != stat2.st_size ||
        compare_timespec(&stat1.st_mtim, &stat2.st_mtim) < 0)
    {
        MFU_LOG(MFU_LOG_VERBOSE, "download: %s", entry);
        rc = ds3sync_download_file(key, abspath);
        if (rc) {
            MFU_LOG(MFU_LOG_ERR, "download fail: %s", entry);
        }
    }
    else {
        MFU_LOG(MFU_LOG_VERBOSE, "skip    : %s", entry);
    }

out:
    mfu_free(&abspath);
    mfu_free(&key);
    return rc;
}

static void ds3sync_init_download(CIRCLE_handle *handle)
{
    int rc = 0;
    strmap *entries = strmap_new();
    const strmap_node *node = NULL;
    char *task = MFU_MALLOC(PATH_MAX + 2);

    rc = s3client_list_tree(s3client, opts->s3_prefix, entries);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to list objects under '%s'. %d:%s",
            opts->remote, -rc, errno2str(-rc));
        goto out;
    }

    if (strmap_size(entries) == 0) {
        /* since we have tested the remote path, the key must be a single object */
        strmap_set(entries, "", "");
    }

    strmap_foreach(entries, node) {
        snprintf(task, PATH_MAX + 2, "D:%s", node->key);
        MFU_LOG(MFU_LOG_DBG, "enqueue task '%s'", task);
        handle->enqueue(task);
    }

out:
    mfu_free(&task);
    strmap_delete(&entries);
    return;
}

static void ds3sync_init_upload(CIRCLE_handle *handle)
{
}

static void ds3sync_add_root(CIRCLE_handle *handle)
{
    if (opts->opcode == DS3SYNC_OPCODE_DOWNLOAD) {
        ds3sync_init_download(handle);
    }
    else if (opts->opcode == DS3SYNC_OPCODE_UPLOAD) {
        ds3sync_init_upload(handle);
    }
    else {
        MFU_LOG(MFU_LOG_ERR, "unknown opcode %d.", opts->opcode);
    }
}

static void ds3sync_process_entry(CIRCLE_handle *handle)
{
    int rc = 0;
    char *task = MFU_MALLOC(PATH_MAX + 2);

    memset(task, 0x00, PATH_MAX + 2);
    handle->dequeue(task);
    MFU_LOG(MFU_LOG_DBG, "dequeue task '%s'", task);

    switch(task[0]) {
        case 'D':
            rc = ds3sync_sync_download_entry(task + 2);
            break;
        default:
            MFU_LOG(MFU_LOG_ERR, "unknown action '%c'", task[0]);
            rc = -EINVAL;
    }

out:
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "failed to process task '%s'. %d:%s",
            task, -rc, errno2str(-rc));
    }
    mfu_free(&task);
    return;
}

int main(int argc, char *argv[])
{
    int rc = 0;
    int rank, ranks;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    mfu_init();
    mfu_debug_level = MFU_LOG_VERBOSE;

    opts = ds3sync_opts_new();
    rc = ds3sync_parse_args(argc, argv);
    if (opts->help) {
        print_usage();
        goto out;
    }
    if (rc) {
        goto out;
    }

    rc = ds3sync_verify_args();
    if (rc) {
        goto out;
    }

    ds3sync_opts_dump(opts);

    MFU_LOG(MFU_LOG_INFO, "ds3sync start...");

    s3client = s3client_new(opts->s3_endpoint,
                            opts->s3_bucket,
                            opts->s3_access_key_id,
                            opts->s3_secret_access_key);
    if (s3client == NULL) {
        rc = -errno;
        goto out;
    }

    /* we need s3client to verify remote path, so it's defered to here */
    if (opts->opcode == DS3SYNC_OPCODE_DOWNLOAD) {
        rc = verify_remote_path();
        if (rc) {
            goto out;
        }
    }

    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;

    MPI_Barrier(MPI_COMM_WORLD);

    CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&ds3sync_add_root);
    CIRCLE_cb_process(&ds3sync_process_entry);

    CIRCLE_begin();
    CIRCLE_finalize();

out:
    s3client_destroy(s3client);
    ds3sync_opts_destroy(&opts);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "Error %d:%s", -rc, errno2str(-rc));
    }

    MFU_LOG(MFU_LOG_INFO, "ds3sync end");
    mfu_finalize();
    MPI_Finalize();

    return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
