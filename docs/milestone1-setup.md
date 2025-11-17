# Milestone 1 – Environment Setup

This document describes:
- AWS EC2 cluster setup (instances, security group).
- Java and Hadoop installation steps.
- HDFS/YARN configuration.
- Screenshots of successful HDFS/YARN startup.

---

## 1. Overview

For this project we deployed a **4-node Hadoop cluster** on **AWS EC2** and configured it in **fully distributed mode**:

- **1 master node** (NameNode + ResourceManager)
- **3 worker nodes** (DataNodes + NodeManagers)

All nodes run **Ubuntu Server 22.04 LTS**, **OpenJDK 11**, and **Hadoop 3.x**.  
The cluster is used to process the **2024 H-1B LCA disclosure data** with MapReduce.

---

## 2. AWS EC2 Cluster Setup

### 2.1 Instances

- **Region:** `us-east-1` (example; adjust if different)
- **Number of instances:** 4
- **Roles & hostnames:**
  - `master` – Hadoop NameNode + ResourceManager
  - `worker1` – DataNode + NodeManager
  - `worker2` – DataNode + NodeManager
  - `worker3` – DataNode + NodeManager
- **AMI:** Ubuntu Server 22.04 LTS (64-bit)
- **Instance type:** e.g., `t3.small` or `t3.medium`
- **Storage:** Each instance has an EBS root volume (gp3) large enough for OS, Hadoop binaries, and HDFS data.

**Screenshot – EC2 instances**

```text
![EC2 instances](screenshots/m1_ec2_instances.png)
```

> *Screenshot: AWS EC2 console showing 4 running instances with names `master`, `worker1`, `worker2`, `worker3`.*

---

### 2.2 Security Group

All 4 instances are attached to a single security group, e.g., `hadoop-cluster-sg`.

**Inbound rules:**

* **SSH**

  * Type: `SSH`
  * Port: `22`
  * Source: `My IP` (developer’s workstation)
* **Cluster internal traffic**

  * Type: `All TCP`
  * Port range: `0–65535`
  * Source: `hadoop-cluster-sg` (the security group itself)

**Optional UI rules (from My IP only):**

* **HDFS NameNode UI:** TCP `9870` → `My IP`
* **YARN ResourceManager UI:** TCP `8088` → `My IP`

**Outbound rules:**

* Default “All traffic → 0.0.0.0/0” (for package downloads, etc.).

**Screenshot – Security group rules**

```text
![Security group](screenshots/m1_security_group.png)
```

> *Screenshot: inbound rules showing SSH from My IP and All TCP from the same security group.*

---

## 3. Java and Hadoop Installation

### 3.1 Java Installation (all nodes)

On **each** instance (`master`, `worker1`, `worker2`, `worker3`):

```bash
sudo apt update
sudo apt -y upgrade

sudo apt install -y openjdk-11-jdk
java -version
```

Expected output includes something like:

```text
openjdk version "11.0.x" 20xx-xx-xx
```

**Screenshot – Java version**

```text
![Java version](screenshots/m1_java_version.png)
```

> *Screenshot: terminal on `master` showing `java -version`.*

---

### 3.2 Hadoop Installation (master, then copied to workers)

On **master**:

```bash
cd ~
wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
tar -xzf hadoop-3.3.6.tar.gz

# Choose install location (example: /usr/local/hadoop)
sudo mv hadoop-3.3.6 /usr/local/hadoop
sudo chown -R ubuntu:ubuntu /usr/local/hadoop
```

Set environment variables in `~/.bashrc` (all nodes):

```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

Apply the changes:

```bash
source ~/.bashrc
hadoop version
```

Then copy Hadoop from the master to each worker:

```bash
ssh worker1 "sudo mkdir -p /usr/local && sudo chown ubuntu:ubuntu /usr/local"
scp -r /usr/local/hadoop worker1:/usr/local/

ssh worker2 "sudo mkdir -p /usr/local && sudo chown ubuntu:ubuntu /usr/local"
scp -r /usr/local/hadoop worker2:/usr/local/

ssh worker3 "sudo mkdir -p /usr/local && sudo chown ubuntu:ubuntu /usr/local"
scp -r /usr/local/hadoop worker3:/usr/local/
```

---

## 4. HDFS and YARN Configuration

All Hadoop configuration files reside in:

```bash
$HADOOP_HOME/etc/hadoop
```

### 4.1 Worker Nodes List

`workers` (on master, then copied to workers):

```text
worker1
worker2
worker3
```

---

### 4.2 `core-site.xml`

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://master:9000</value>
    </property>
</configuration>
```

---

### 4.3 `hdfs-site.xml`

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>

    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///usr/local/hadoop/data/nn</value>
    </property>

    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///usr/local/hadoop/data/dn</value>
    </property>
</configuration>
```

Create HDFS data directories:

```bash
# master
ssh master
sudo mkdir -p /usr/local/hadoop/data/nn /usr/local/hadoop/data/dn
sudo chown -R ubuntu:ubuntu /usr/local/hadoop/data

# workers
ssh worker1 "sudo mkdir -p /usr/local/hadoop/data/dn && sudo chown -R ubuntu:ubuntu /usr/local/hadoop/data"
ssh worker2 "sudo mkdir -p /usr/local/hadoop/data/dn && sudo chown -R ubuntu:ubuntu /usr/local/hadoop/data"
ssh worker3 "sudo mkdir -p /usr/local/hadoop/data/dn && sudo chown -R ubuntu:ubuntu /usr/local/hadoop/data"
```

---

### 4.4 `mapred-site.xml`

```xml
<configuration>
    <!-- Run MapReduce on YARN -->
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>

    <!-- Tell AM and tasks where the Hadoop install lives -->
    <property>
        <name>yarn.app.mapreduce.am.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>

    <property>
        <name>mapreduce.map.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>

    <property>
        <name>mapreduce.reduce.env</name>
        <value>HADOOP_MAPRED_HOME=/usr/local/hadoop</value>
    </property>
</configuration>
```

---

### 4.5 `yarn-site.xml`

```xml
<configuration>
    <!-- ResourceManager on master -->
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>master</value>
    </property>

    <!-- Enable MapReduce shuffle service on NodeManagers -->
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>

    <property>
        <name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
        <value>org.apache.hadoop.mapred.ShuffleHandler</value>
    </property>
</configuration>
```

After editing the configs on `master`, the entire config directory is copied to workers:

```bash
cd /usr/local/hadoop/etc
for host in worker1 worker2 worker3; do
  scp -r hadoop $host:/usr/local/hadoop/etc/
done
```

---

## 5. HDFS/YARN Startup and Validation

### 5.1 Starting HDFS and YARN

On the master:

```bash
start-dfs.sh
start-yarn.sh
```

Check Java processes:

```bash
jps
```

* On `master`: `NameNode`, `SecondaryNameNode`, `ResourceManager`, `Jps`.
* On workers: `DataNode`, `NodeManager`, `Jps`.

### 5.2 HDFS Safe Mode and User Directory

```bash
hdfs dfsadmin -safemode get
hdfs dfsadmin -safemode leave

hdfs dfs -mkdir -p /user/ubuntu
hdfs dfs -ls /user
```

### 5.3 Built-in MapReduce Example (π Estimation)

To verify MapReduce + YARN integration:

```bash
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
  pi 2 10
```

Successful completion confirms that the ApplicationMaster and YARN containers are correctly configured.

