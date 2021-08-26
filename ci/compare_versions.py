# should script force only major, minor, and patch to have values (no prerelease or build?)
# program doesn't catch if -de.3 used instead of -dev.3

import semver
import pdb

def compare_versions(BATCHKIT_NEW, BATCHKIT_OLD):
    try:
        if semver.compare(BATCHKIT_NEW, BATCHKIT_OLD) == 1:    # returns 1 if major, minor, patch, prerelease, and/or build version incremented in BATCHKIT_NEW compared to BATCHKIT_OLD
            print("1")
        else:
            print("0")
    except:
        print("-1")
# version = "0.9.9-dev.0"
# version = "0.9.12-dev.2"
# inc_major = "1.9.12-dev.2"
# inc_minor = "0.10.12-dev.2"
# inc_patch = "0.9.13-dev.2"
# inc_prerelease = "0.9.12-dev.9"
# inc_many = "0.10.1"

# semver.VersionInfo.parse(version)

# semver.compare(inc_prerelease, version)
# semver.compare(inc_many, version)
# pdb.set_trace()