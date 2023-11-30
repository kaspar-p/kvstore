#pragma once

#include <string>

#include "buf.hpp"
#include "naming.hpp"

DbNaming create_dir(std::string dir_name);

BufPool test_buf();
