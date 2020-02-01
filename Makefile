CPPFLAGS=-O2 -g -Wall -Isrc/libhdfs/
LDFLAGS=-L$HADOOP_HOME/lib/native -lhdfs

all: hdfs

hdfs: src/hdfs.o
	g++ $(CPPFLAGS) $(LDFLAGS) -o hdfs src/hdfs.cpp

clean:
	rm -rf *.o *~ hdfs
