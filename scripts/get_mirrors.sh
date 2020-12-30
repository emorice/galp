#!/bin/bash
# Print the list of remote destination we want to mirror the project to

if [ -f 'scripts/local/get_mirrors.sh' ]; then 
	. scripts/local/get_mirrors.sh
fi
