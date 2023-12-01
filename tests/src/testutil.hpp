#pragma once

#include <string>

#include "buf.hpp"
#include "naming.hpp"

DbNaming create_dir(std::string dir_name);

BufPool test_buf();

void structure_exists(std::string prefix, uint8_t tiers, int level, int run,
                      int intermediate);
void structure_exists(std::string prefix, uint8_t tiers, int level, int run);
void structure_exists(std::string prefix, uint8_t tiers, int level);

void structure_not_exists(std::string prefix, uint8_t tiers, int level, int run,
                          int intermediate);
void structure_not_exists(std::string prefix, uint8_t tiers, int level,
                          int run);
void structure_not_exists(std::string prefix, uint8_t tiers, int level);
