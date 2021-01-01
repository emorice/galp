Top pipeline
============

Code and tools to have a single-entry point and dashboard for all analyses.

Tools that are specific to one analysis step can be factored away to their own
projects as they grow.

How does it looks like ?
------------------------

This repo should host a bunch of scripts to control:
 * Starting, stopping and monitoring the servers needed for the analyses. Many
   such servers are actually needed in a distributed pipeline, such as a central
   scheduler, workers, database servers, in-memory and on-disk remote access to
   resources, dashboards, etc. Many aspect of these scripts will be
   site-dependent, but we try to separate the common logic (what to start) from
   the local aspects (where and how to start it)
 * Loading individual pipeline graphs and interactively develop new tasks to
   include in them.
 * Trigger and monitor the execution of such pipelines.

Where does it run ?
-------------------

The top-level code runs on the end-user client, ideally a desktop with decent
resources. Shared filesystems, file syncing and just-in-time code serializing
are then used to forward the parts that are meant to be remotely executed on
other machines.

To reduce dependency on end-user system, we rely instead on it being a
singularity host, and start components of the top-level client as
containerized services. This way deploying the client on an other system can be
kept simple (ensure singularity is installed, clone the repo, make sure you have
all the necessary ssh keys and hosts, start, enjoy).

Layers
------

The first layer on the pipeline rely on existing communication services present
in the infrastructure, such as:
 * ssh access from one machine to another
 * slurm scheduler
 * shared filesystems

These elements are used to setup a higer-level layer of python processes, which
provide scheduling and execution service to the client. Thus, the client never
needs to issue ssh commands, transfer files or submit slurm scripts, this is
all done at python level, which allows more flexible and interactive management
of resources and tasks, and, most importantly, a unified interface to
heterogeneous compute nodes (inside/outside of a cluster, etc).

How is this repo organized ?
----------------------------

 * `requirements/`: various subsets of dependencies that are required:
	* `runtime.txt`: on all hosts/containers that are part of the
	  distributed system,
	* `dev.txt`: (includes runtime): on hosts that run test and debug code
 * `scripts/` contains various startup procedures. As a rule, they are meant to
   be run from the repo root.
 * `config`: what it says


Components of the top-level client :
------------------------------------

 * a lsyncd service

Dependency tracking
-------------------

As a rule, all versions of all tools must be specified. A streamlined workflow
for bumping the versions and testing will be added later.
