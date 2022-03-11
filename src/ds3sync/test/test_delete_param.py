#/usr/bin/python3

import pytest
import os, shutil
import filecmp
from functions import *


def test_download_delete_file():
	"""
	Test remove extra file on target
	"""
	fn1 = "%s/empty-file-1" % testdir
	fn2 = "%s/empty-file-2" % testdir
	fn3 = "%s/d1/d2/d3/empty-file-4" % testdir

	touch_file(fn1)

	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_aws_s3_sync(sync_args)

	touch_file(fn2)
	os.makedirs(os.path.dirname(fn3))
	touch_file(fn3)

	assert(os.path.exists(fn2))
	assert(os.path.exists(fn3))

	sync_args = ["--delete", "s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	assert(os.path.isfile(fn1))
	assert(not os.path.exists(fn2))
	assert(not os.path.exists(fn3))

def test_upload_delete_file():
	"""
	Test remove extra file on target
	"""
	fn1 = "%s/empty-file-1" % testdir
	fn2 = "%s/empty-file-2" % testdir
	fn3 = "%s/d1/d2/d3/empty-file-4" % testdir

	touch_file(fn1)
	touch_file(fn2)
	os.makedirs(os.path.dirname(fn3))
	touch_file(fn3)

	sync_args = ["--delete", testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	os.unlink(fn2)
	os.unlink(fn3)
	run_ds3sync(sync_args)

	shutil.rmtree(testdir)
	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_aws_s3_sync(sync_args)

	assert(os.path.isfile(fn1))
	assert(not os.path.exists(fn2))
	assert(not os.path.exists(fn3))

