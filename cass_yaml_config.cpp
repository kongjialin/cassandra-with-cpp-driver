#include "storage/yamlcpp/yaml.h"

#include <fstream>

#include "thirdparty/gflags/gflags.h"

DEFINE_string(seeds, "127.0.0.1",
              "Addresses of comma delimited hosts "
              "that are deemed contact points");
DEFINE_string(cass_ip, "127.0.0.1", "ip address of the cassandra server");
DEFINE_int32(native_transport_port, 9042,
             "port for CQL native transport to listen for clients on");
DEFINE_int32(storage_port, 7005, "TCP port, for commands and data");

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, false);

  YAML::Node config = YAML::LoadFile("cassandra.yaml");

  config["data_file_directories"][0] = "/var/lib/cassandra/data";
  config["commitlog_directory"] = "/var/lib/cassandra/commitlog";
  config["saved_caches_directory"] = "/var/lib/cassandra/saved_caches";

  YAML::Node seed_provider = config["seed_provider"];
  YAML::Node parameters = seed_provider[0]["parameters"];
  parameters[0]["seeds"] = FLAGS_seeds;

  config["listen_address"] = FLAGS_cass_ip;
  config["rpc_address"] = FLAGS_cass_ip;

  config["storage_port"] = FLAGS_storage_port;
  config["native_transport_port"] = FLAGS_native_transport_port;

  std::ofstream fout("cassandra.yaml");
  fout << config;
  return 0;
}
