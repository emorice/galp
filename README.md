Top pipeline
============

Code and tools to have a single-entry point and dashboard for all analyses.

Tools that are specific to one analysis step can be factored away to their own
projects as they grow.

How does it looks like ?
------------------------

This repo should host a bunch of python modules that are meant to be loaded from
an Ipython (Jupyter) notebook, and control:
 * Starting, stopping and monitoring the servers needed for the analyses. Many
   such servers are actually needed in a distributed pipeline, such as a central
   scheduler, workers, database servers, in-memory and on-disk remote access to
   resources, dashboards, etc.
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
in the infrastructure:
 * ssh access from one machine to another
 * slurm scheduler
 * shared filesystems

These elements are used to setup a higer-level layer of python processes, which
provide scheduling and execution service to the client. Thus, the client never
needs to issue ssh commands, transfer files or submit slurm scripts, this is
all done at python level, which allows more flexible and interactive management
of resources and tasks, and, most importantly, a unified interface to
heterogeneous compute nodes (inside/outside of a cluster, etc)

How is this repo organized ?
----------------------------

 * `requirements.txt`: python dependencies for the top client
 * `requirements/`: various subsets of dependencies that needs to be present
   	on the workers, but can be different from the top client (e.g. workers
   	never need a jupyter server, though this is not a good example since we
	usually work in containers with jupyter preinstalled)
 * `scripts/` contains various startup procedures. As a rule, they are meant to
   be run from the repo root.
 * `config`: what it says


Components of the top-level client :
------------------------------------

 * a jupyter server
 * a dask client
 * a web server (flask/dash)
 * a lsyncd service

Dependency tracking
-------------------

As a rule, all versions of all tools must be specified. A streamlined workflow
for bumping the versions and testing will be added later.
