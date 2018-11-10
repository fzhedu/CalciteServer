// gen cpp file src/gen-cpp
thrift -r --gen cpp CalciteServer.thrift
cd gen-cpp
g++ -g -std=c++11  -I/usr/local/include/thrift -I/usr/local/include/boost -I./  CalciteServer.cpp CalciteServer_types.cpp CalciteServer_constants.cpp ../main/cpp/SqlClient.cpp -o client -lthrift

