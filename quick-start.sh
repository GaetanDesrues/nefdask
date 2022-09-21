#!/bin/bash

CUR=$(dirname "$(realpath $0)")

DEFAULT="packageName"
PACKAGE_NAME=$1  # "MyPackageName"  # Place your new package name here

mv $CUR/$DEFAULT $CUR/$PACKAGE_NAME
find $CUR -type f -print0 | xargs -0 sed -i "s/$DEFAULT/$PACKAGE_NAME/g"