# hadoop-fs

The hadoop-fs project is designed to create a new default Hadoop compatible filesystem that combines the HDFS and S3A filesystems to move the primary storage to S3 while maintaining most of the HDFS semantics (authentication, authorization, etc).  In an effort to remove the need for new client side code the system is designed to work within a deployed HttpFS instance.
