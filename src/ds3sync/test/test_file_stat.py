#/usr/bin/python3

import pytest
import os
import stat
from functions import *


def test_file_stat():
	"""
	Test file stat (uid/gid/perm)
	"""
	fn = "%s/test-file-stat-1" % testdir
	touch_file(fn)
	os.chown(fn, 500, 501);
	os.chmod(fn, 0o750);
	sync_args = [testdir, "s3://%s/%s" % (s3_bucket, testdir)]
	run_ds3sync(sync_args)

	shutil.rmtree(testdir)

	sync_args = ["s3://%s/%s" % (s3_bucket, testdir), testdir]
	run_ds3sync(sync_args)

	file_stat = os.stat(fn);
	assert(file_stat.st_uid == 500);
	assert(file_stat.st_gid == 501);
	perm_mask = (stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
	assert((file_stat.st_mode & perm_mask) == 0o750)

