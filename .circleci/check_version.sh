#!/bin/sh

# Get git tag, delete v affix
tag=$(git tag -l --contains HEAD | tr -d v)

# Get project version
version=$(cat pyproject.toml | grep "^version = \"*\"" | cut -d'"' -f2)

# Check if tag matches version
if [ -z "$tag" ]; then
    echo Git tag is empty!
    exit 1
elif [ "$tag" != "$version" ]; then
    echo Git tag $tag does not match version $version
    exit 1
else
    echo Git tag $tag matches version $version
    echo Deploying...
fi