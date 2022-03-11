#/usr/bin/python3

import pytest
import os, shutil
import filecmp
import time
from functions import *


def test_upload_empty_file():
	"""
	Test download of empty dir.
	"""
	fn = "%s/empty-file-1" % testdir
	touch_file(fn)

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	shutil.rmtree(testdir)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_aws_s3_sync(sync_args)

	assert(os.path.isfile(fn))


def test_upload_small_file():
	"""
	Test download of small dir.
	"""
	fn = "%s/small-file-1" % testdir
	fn2 = "%s/small-file-2" % testdir

	with open(fn, "w") as file_handler:
		file_handler.write("This is a small file")

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	os.rename(fn, fn2)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_aws_s3_sync(sync_args)

	assert(filecmp.cmp(fn, fn2))


def test_upload_large_file():
	"""
	Test download of large dir.
	"""
	fn = "%s/large-file-1" % testdir
	fn2 = "%s/large-file-2" % testdir

	subprocess.run(["dd", "if=/dev/urandom", "of=%s" % fn, "bs=1K", "count=1001"])

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	os.rename(fn, fn2)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_aws_s3_sync(sync_args)

	assert(filecmp.cmp(fn, fn2))


def test_upload_subdir_file():
	"""
	Test download of empty dir.
	"""
	dir = "%s/d1/d2/d3" % testdir
	fn = "%s/empty-file-4" % dir

	os.makedirs(dir, exist_ok=True)
	touch_file(fn)

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	shutil.rmtree(testdir)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_aws_s3_sync(sync_args)

	assert(os.path.isfile(fn))


def test_sync_upload_file_by_size():
	"""
	Test sync download
	"""
	fn = "%s/small-file-1" % testdir
	fn2 = "%s/small-file-2" % testdir

	with open(fn, "w") as file_handler:
		file_handler.write("This is a small file")

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	os.rename(fn, fn2);
	with open(fn, "w") as file_handler:
		file_handler.write("This is a different small file")

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_aws_s3_sync(sync_args)

	assert(filecmp.cmp(fn, fn2))


def test_sync_upload_file_by_mtime():
	"""
	Test sync download
	"""
	fn = "%s/small-file-1" % testdir
	fn2 = "%s/small-file-2" % testdir

	with open(fn, "w") as file_handler:
		file_handler.write("This is a small file")

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	os.rename(fn, fn2);
	with open(fn, "w") as file_handler:
		file_handler.write("This is a large file")

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_aws_s3_sync(sync_args)

	assert(filecmp.cmp(fn, fn2))

