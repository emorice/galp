#!/bin/bash

# Starts a containerized dask scheduler

# Just as several other scripts, this is a recursive script that when called
# outside of a container, will start one and start itself inside it. This avoids
# having pairs of scripts when both are tightly coupled to perform a single
# function

# CLONEID environment variable is assumed to be set

# If we cannot find singularity, maybe we need to load it first
# todo: unhardcode version
ensure_singularity() {
	if ! singularity version >& /dev/null ; then
		module load singularity/3.2.1
	fi
}

# ensure we store the fs on local disk
fix_tempdir() {
	# on some nodes, temporary directory is broken
	if [ -d "$TMP_LOCAL" ] ; then
		export SINGULARITY_TMPDIR="$TMP_LOCAL"
	else
		export SINGULARITY_TMPDIR="/local_hdd/jobs/$SLURM_JOBID"
	fi
}
		
start_container() {
		mkdir -p "${SINGULARITY_TMPDIR}/work"
		singularity exec \
			-C -e \
			-B "${HOME}/gtop_${CLONEID}":/var/project:ro \
			-B "${SINGULARITY_TMPDIR}/work":/work:rw \
			docker://jupyter/scipy-notebook:399cbb986c6b \
			/var/project/scripts/dask_scheduler.sh \
				--contained
}

python_venv() {
	python -m venv --system-site-packages /work/venv
	. /work/venv/bin/activate 
	pip install -r /var/project/requirements/slave.txt
}

main() {
	while [ "$1" ]; do
		case "$1" in
			'--contained')
				CONTAINED=1
			;;
		esac
		shift
	done

	if [ $CONTAINED ]; then
		python_venv
		echo "Hello from inside"
		dask-scheduler
	else
		echo "Starting container"
		ensure_singularity
		fix_tempdir
		start_container
	fi
}

main "$@"

