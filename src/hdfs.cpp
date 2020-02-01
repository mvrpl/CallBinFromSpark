#include "uri.hh"
#include "hdfs.h"
#include <utility>
#include <iostream>
#include <stdexcept>
#include <string>

using namespace std;

int main(int argc, char **argv){
	string input_path;
	cin >> input_path;
	string strInFS = argv[1];
	uri uriin(strInFS);
	string strOutFS = argv[2];
	uri uriout(strOutFS);
	string output_path = argv[3];

	pair<string, int> srcFS = make_pair(uriin.get_host(), (int)uriin.get_port());
	hdfsFS fsIn = hdfsConnect(&srcFS.first[0], srcFS.second);
	pair<string, int> dstFS = make_pair(uriout.get_host(), (int)uriout.get_port());
	hdfsFS fsOut = hdfsConnect(&dstFS.first[0], dstFS.second);

	hdfsCopy(fsIn, &input_path[0], fsOut, &output_path[0]);
}
