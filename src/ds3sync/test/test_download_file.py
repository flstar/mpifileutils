#/usr/bin/python3

import pytest
import os, shutil
import filecmp
import time
from functions import *


def test_download_empty_file():
	"""
	Test download of empty dir.
	"""
	fn = "%s/empty-file-1" % testdir
	touch_file(fn)

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	shutil.rmtree(testdir)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	assert(os.path.isfile(fn))


def test_download_small_file():
	"""
	Test download of small dir.
	"""
	fn = "%s/small-file-1" % testdir
	fn2 = "%s/small-file-2" % testdir

	with open(fn, "w") as file_handler:
		file_handler.write("This is a small file")

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	os.rename(fn, fn2)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	assert(filecmp.cmp(fn, fn2))


def test_download_large_file():
	"""
	Test download of large dir.
	"""
	fn = "%s/large-file-1" % testdir
	fn2 = "%s/large-file-2" % testdir

	subprocess.run(["dd", "if=/dev/urandom", "of=%s" % fn, "bs=1K", "count=1001"])

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	os.rename(fn, fn2)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	assert(filecmp.cmp(fn, fn2))


def test_download_subdir_file():
	"""
	Test download of empty dir.
	"""
	dir = "%s/d1/d2/d3" % testdir
	fn = "%s/empty-file-4" % dir

	os.makedirs(dir, exist_ok=True)
	touch_file(fn)

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	shutil.rmtree(testdir)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	assert(os.path.isfile(fn))


def test_sync_download_file_by_size():
	"""
	Test sync download
	"""
	fn = "%s/small-file-1" % testdir
	fn2 = "%s/small-file-2" % testdir

	with open(fn, "w") as file_handler:
		file_handler.write("This is a small file")

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	os.rename(fn, fn2);
	with open(fn, "w") as file_handler:
		file_handler.write("This is a different small file")

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	assert(filecmp.cmp(fn, fn2))


def test_sync_download_file_by_mtime():
	"""
	Test sync download
	"""
	fn = "%s/small-file-1" % testdir
	fn2 = "%s/small-file-2" % testdir

	with open(fn, "w") as file_handler:
		file_handler.write("This is a small file")

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	os.rename(fn, fn2);
	with open(fn, "w") as file_handler:
		file_handler.write("This is a large file")

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	assert(filecmp.cmp(fn, fn2))


def test_download_overwrite_param():
	"""
	Test download overwrite_param
	"""
	fn = "%s/empty-file-1" % testdir
	touch_file(fn)

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	# change the content of file so that the downloading does happen
	with open(fn, "w") as file_handler:
		file_handler.write("anything")

	ino1 = os.stat(fn).st_ino
	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	# since the file is renamed from a temporary file, its ino should have changed
	ino2 = os.stat(fn).st_ino
	assert(ino1 != ino2)

	# change the content of file so that the downloading does happen
	with open(fn, "w") as file_handler:
		file_handler.write("anything more")

	ino1 = os.stat(fn).st_ino
	# now we add --overwrite param and expect the ino of file will NOT change
	sync_args = ["--overwrite", "s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	ino2 = os.stat(fn).st_ino
	assert(ino1 == ino2)

