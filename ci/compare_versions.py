# versions must have format similar to: 0.9.13-dev.2

import semver

def compare_versions(BATCHKIT_NEW, BATCHKIT_OLD):
    try:
        if semver.compare(BATCHKIT_NEW, BATCHKIT_OLD) == 1:    # returns 1 if major, minor, patch, prerelease, and/or build version incremented in BATCHKIT_NEW compared to BATCHKIT_OLD
            print("1")
        else:
            print("0")
    except:
        print("-1")