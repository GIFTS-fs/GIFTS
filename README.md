# README

We propose a design for
GIFTS (Go Immutable File Transmission System),
a block-based distributed file system that focuses on dynamic
replica assignment based on file traffic.
The overall structure of GIFTS is inspired by
the Hadoop file system (HDFS) \cite{10.1109/MSST.2010.5496972}:
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
varies. We value both transparency and performance as users
should never directly notice the dynamic replication, except for the
potential performance improvement.

We show that, when targeting write-once read-many workloads,
our system can achieve a throughput of approximately 119 MBps
per Storage node in the AWS EC2 networking environment,
consuming nearly the entire available network bandwidth.
Our results indicate that GIFTS is highly scalable and
achieves a nearly ideal linear increase in network bandwidth usage
as hot files are dynamically replicated.

A style-free coding documentation can be found at [here](https://gifts-fs.github.io/GIFTS/index.html).
To have the full `godoc` experience, feel free to clone the repo and start `godoc` server locally.
