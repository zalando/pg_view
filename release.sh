#!/bin/sh

if [ $# -ne 1 ]; then
    >&2 echo "usage: $0 <version>"
    exit 1
fi

set -xe

python --version
git --version

version=$1

sed -i "s/__version__ = .*/__version__ = '${version}'/" pg_view.py
python setup.py clean
python setup.py test
python setup.py flake8

git add pg_view.py

git commit -m "Bumped version to $version"
git push

python setup.py sdist upload

git tag ${version}
git push --tags
