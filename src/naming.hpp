#pragma once

#include <filesystem>
#include <string>

struct DbNaming {
  std::filesystem::path dirpath;
  std::string name;
};

std::string manifest_file(const DbNaming& naming);

std::string data_file(const DbNaming& naming, int level, int run,
                      int intermediate);
int parse_data_file_level(const std::string& filename);
int parse_data_file_run(const std::string& filename);
int parse_data_file_intermediate(const std::string& filename);

std::string filter_file(const DbNaming& naming, int level, int run,
                        int intermediate);
int parse_filter_file_level(const std::string& filename);
int parse_filter_file_run(const std::string& filename);
int parse_filter_file_intermediate(const std::string& filename);

std::string lock_file(const DbNaming& naming);