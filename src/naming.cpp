#include "naming.hpp"

#include <cassert>
#include <regex>
#include <string>

std::string manifest_file(const DbNaming& naming) {
  return naming.dirpath / (naming.name + ".MANIFEST");
}

std::string data_file(const DbNaming& naming, int level, int run,
                      int intermediate) {
  return naming.dirpath /
         (naming.name + ".DATA.L" + std::to_string(level) + ".R" +
          std::to_string(run) + ".I" + std::to_string(intermediate));
}

int parse_data_file_level(const std::string& filename) {
  std::smatch m;
  std::regex_match(filename, m, std::regex(R"(^.*\.DATA\.L(\d+).*$)"));
  assert(m.size() == 2);

  int level = std::stoull(m[1].str());
  return level;
}

int parse_data_file_run(const std::string& filename) {
  std::smatch m;
  std::regex_match(filename, m, std::regex(R"(^.*\.DATA\.L\d+\.R(\d+).*$)"));
  assert(m.size() == 2);

  int level = std::stoull(m[1].str());
  return level;
}

int parse_data_file_intermediate(const std::string& filename) {
  std::smatch m;
  std::regex_match(filename, m,
                   std::regex(R"(^.*\.DATA\.L\d+\.R\d+\.I(\d+)$)"));
  assert(m.size() == 2);

  int level = std::stoull(m[1].str());
  return level;
}

std::string filter_file(const DbNaming& naming, int level, int run,
                        int intermediate) {
  return naming.dirpath /
         (naming.name + ".FILTER.L" + std::to_string(level) + ".R" +
          std::to_string(run) + ".I" + std::to_string(intermediate));
}

int parse_filter_file_level(const std::string& filename) {
  std::smatch m;
  std::regex_match(filename, m, std::regex(R"(^.*\.FILTER\.L(\d+).*$)"));
  assert(m.size() == 2);

  int level = std::stoull(m[1].str());
  return level;
}

int parse_filter_file_run(const std::string& filename) {
  std::smatch m;
  std::regex_match(filename, m, std::regex(R"(^.*\.FILTER\.L\d+\.R(\d+).*$)"));
  assert(m.size() == 2);

  int level = std::stoull(m[1].str());
  return level;
}

int parse_filter_file_intermediate(const std::string& filename) {
  std::smatch m;
  std::regex_match(filename, m,
                   std::regex(R"(^.*\.FILTER\.L\d+\.R\d+\.I(\d+)$)"));

  assert(m.size() == 2);

  int level = std::stoull(m[1].str());
  return level;
}

std::string lock_file(const DbNaming& naming) {
  return naming.dirpath / (naming.name + ".LOCK");
}