CPPFLAGS=-O2 -g -Wall -Isrc/libhdfs/
LDFLAGS=-L$$HADOOP_HOME/lib/native -lhdfs

all: clean hdfs

hdfs: src/hdfs.cpp
	g++ $(CPPFLAGS) $(LDFLAGS) $^ -o $@

clean:
	rm -rf *.o *~ hdfs
