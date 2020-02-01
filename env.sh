#!/bin/bash

export HADOOP_HOME=`find / -name "libhadoop.so" 2> /dev/null | sed 's|/lib/native/libhadoop.so||g'`
export CLASSPATH=`hadoop classpath --glob`
