#!/usr/bin/python3

import os, subprocess, shutil


installdir = "/prj/mpifileutils/install"
workdir    = "pytest.dir"
testdir    = "testdir"

s3_endpoint         = "http://localhost:9000"
s3_access_key_id    = "minioadmin"
s3_secret_access_key= "minioadmin"
s3_bucket           = "bucket-1"


def run_aws_s3(sublist):
	my_env = {**os.environ, 'AWS_ACCESS_KEY_ID': s3_access_key_id,
				'AWS_SECRET_ACCESS_KEY': s3_secret_access_key}
	cmdlist = ["aws", "--endpoint-url=%s" % s3_endpoint, "s3"] + sublist
	subprocess.run(cmdlist, env=my_env)


def run_aws_s3_sync(sublist):
	run_aws_s3(["sync",] + sublist)


def run_ds3sync(sublist):
	my_env = {**os.environ,
				'LD_LIBRARY_PATH': "%s/lib:%s" % (installdir, os.environ.get('LD_LIBRARY_PATH'))}
	cmdlist = ["%s/bin/ds3sync" % installdir,
				"--s3-endpoint=%s" % s3_endpoint,
				"--s3-access-key-id=%s" % s3_access_key_id,
				"--s3-secret-access-key=%s" % s3_secret_access_key
			  ] + sublist
	print(cmdlist)
	subprocess.run(cmdlist, env=my_env)


def setup_function():
	print("Prepare workdir: %s" % workdir)
	if os.path.exists(workdir):
		shutil.rmtree(workdir)
	os.mkdir(workdir)
	os.chdir(workdir)
	os.mkdir(testdir)

	uri = "s3://%s/%s" % (s3_bucket, testdir)
	sublist = ["rm", "--recursive", uri]
	run_aws_s3(sublist)


def teardown_function():
	print("Remove workdir tree: %s" % workdir)
	os.chdir("..")
	if os.path.exists(workdir):
		shutil.rmtree(workdir)

	uri = "s3://%s/%s" % (s3_bucket, testdir)
	sublist = ["rm", "--recursive", uri]
	run_aws_s3(sublist)


def touch_file(filename):
	fid = open(filename, "w");
	fid.close()
	os.utime(filename)

