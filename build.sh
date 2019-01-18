#!/bin/bash
make genversion
docker build -t bungie-manifest-updater:$(bash generate_version.sh) .
