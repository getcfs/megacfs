#!/bin/bash

# Verify that all files are correctly golint'd, with the exception of
# generated code.
EXIT=0
GOLINT=$(golint ./... | grep -v "internal/nl80211/")

if [[ ! -z $GOLINT ]]; then
	echo "$GOLINT"
	EXIT=1
fi

exit $EXIT
