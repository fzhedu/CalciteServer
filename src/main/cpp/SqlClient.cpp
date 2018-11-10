#include "../../gen-cpp/CalciteServer.h"
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>
#include <string>
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;

int main(int argc, char **argv) {
  ::apache::thrift::stdcxx::shared_ptr<TSocket> socket(
      new TSocket("localhost", 8099));
  ::apache::thrift::stdcxx::shared_ptr<TTransport> transport(
      new TFramedTransport(socket));
  ::apache::thrift::stdcxx::shared_ptr<TProtocol> protocol(
      new TBinaryProtocol(transport));

  transport->open();

  CalciteServerClient client(protocol);
  for (int i = 0; i < 10; ++i) {
    TPlanResult result;
    client.sql2Plan(result, "fzh", "123456", "default", "select * from MYTABLE",
                    true, false);
    printf("Thrift client result=%s", result.plan_result.c_str());
  }

  transport->close();
  return 0;
}
