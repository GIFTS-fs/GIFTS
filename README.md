# Abstract
We propose a design for
GIFTS (Go Immutable File Transmission System),
a block-based distributed file system that focuses on dynamic
replica assignment based on traffic.
The overall structure of GIFTS is inspired by
Hadoop's file system (HDFS) \cite{10.1109/MSST.2010.5496972}:
a centralized **Master** manages mappings from file names to a list
of **Storage** nodes that contain its respective blocks.
The **Client** talks to the Master first during any file access
and then retrieves or writes data directly from or to the Storages.

Users interact with GIFTS via Clients which can create 
and read files. When creating a file, users assign a value
indicating its relative expected popularity,
which we denote as the *replication factor*.
The replication factor determines
the initial number of replicas of a given file
with larger values indicating an expected
larger volume of traffic.

GIFTS aims to improve the system's throughput by
dynamically adjusting the number of replicas for a given file.
Since the Master maintains a centralized record of all traffic,
it is natural for it to adjust
the number of replicas for files as their popularity
varies.  We value both transparency and performance as users
should never directly notice the dynamic replication, except for the
potential performance improvement.

We show that, when targeting write-once read-many workloads,
our system can achieve a throughput of approximately 119 MBps
per Storage node in the AWS EC2 networking environment,
consuming nearly the entire available network bandwidth.
Our results indicate that GIFTS is highly scalable and
achieve a nearly ideal linear increase in network bandwidth usage as hot
files are dynamically replicated.

# Introduction
Users today have access to various mature distributed file systems,
many of which are fast, reliable, and provide high availability.
Besides those goals, load balancing has been under the spotlight as well for
large, scaled systems. Given that the load changes dynamically,
providing load balancing at runtime also becomes appealing.
For many applications it is common for a small set
of files to be very popular
(for example, metadata files or recently created data) with this
set varying over time.
By dynamically distributing the traffic
for these hot files among additional nodes we expect to
both decrease average machine load while simultaneously decreasing
response latency and increasing throughput.

In this paper we propose a design for
GIFTS (Go Immutable File Transmission System),
a block-based distributed file system that focuses on dynamic
replica assignment based on traffic and user configuration.
GIFTS employs a simplified HDFS-style architecture by separating 
the metadata from the content \cite{10.1109/MSST.2010.5496972}.
Each file in the system is further divided into
blocks that are distributed across the various Storage nodes in the system.
A user must specify a file's *replication factor* during its creation
to provide a hint to the system as to likely popularity of the file.
Larger replication factors imply a belief in the fact that the file will
receive a larger share of the system's traffic.

Clients interact with a central Master to retrieve the metadata on every file access.
The Master utilizes this information to monitor the global traffic across all Storage
nodes to determine when a given file's popularity warrants extra replicas.
As an optimization, the Master offloads the actual data reads and writes,
including the replication of existing files, to the Clients and Storage nodes.
This produces a highly scalable infrastructure that 
exploits the full potential of the available network bandwidth
with minimal overhead.
