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


#define S3_SCHEME                           "s3://"

#define DS3SYNC_OPT_S3_ENDPOINT             1
#define DS3SYNC_OPT_S3_ACCESS_KEY_ID        2
#define DS3SYNC_OPT_S3_SECRET_ACCESS_KEY    3

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
} ds3sync_opts_t;
static ds3sync_opts_t *opts;

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
                *path, -rc, strerror(-rc));
        goto out;
    }

    if (isdir) {
        /* check local path is a dir */
        struct stat statbuf;
        rc = lstat(rpath, &statbuf);
        if (rc) {
            rc = -errno;
            MFU_LOG(MFU_LOG_ERR, "failed to stat local path '%s'. %d:%s",
                *path, -rc, strerror(-rc));
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

out:
    return rc;
}

static void ds3sync_add_root(CIRCLE_handle *handle)
{
    return;
}

static void ds3sync_process_entry(CIRCLE_handle *handle)
{
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

    CIRCLE_loglevel CIRCLE_debug = CIRCLE_LOG_WARN;

    MPI_Barrier(MPI_COMM_WORLD);

    CIRCLE_init(argc, argv, CIRCLE_DEFAULT_FLAGS);
    CIRCLE_cb_create(&ds3sync_add_root);
    CIRCLE_cb_process(&ds3sync_process_entry);

    CIRCLE_begin();
    CIRCLE_finalize();

out:
    ds3sync_opts_destroy(&opts);
    if (rc) {
        MFU_LOG(MFU_LOG_ERR, "Error %d:%s", -rc, strerror(-rc));
    }

    MFU_LOG(MFU_LOG_INFO, "ds3sync end");
    mfu_finalize();
    MPI_Finalize();

    return rc ? EXIT_FAILURE : EXIT_SUCCESS;
}
