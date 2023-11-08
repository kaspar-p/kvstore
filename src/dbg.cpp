#include "dbg.hpp"

#include <cassert>
#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

std::string repeat(const std::string& input, uint32_t num) {
  std::string ret;
  ret.reserve(input.size() * num);
  while (num--) {
    ret += input;
  }
  return ret;
}

std::string bit_string(uint32_t num, uint32_t bits) {
  std::ostringstream s;

  for (int j = 0; j < bits; j++) {
    s << std::to_string((num >> (32 - j - 1)) & 1);
  }

  std::ostringstream s2;
  for (int i = bits - s.str().size() - 1; i >= 0; i--) {
    s2 << '0';
  }
  s2 << s.str();

  return s2.str();
}

[[nodiscard]] uint8_t prefix_bit(uint32_t prefix, uint32_t bit_offset) {
  int bit = (prefix >> (32 - bit_offset - 1)) & 0b1;
  assert(bit == 0 || bit == 1);
  return bit;
}

std::vector<std::string> split_string(std::string s, std::string&& delimiter) {
  size_t pos = 0;
  std::vector<std::string> tokens;
  while (pos < s.size()) {
    pos = s.find(delimiter);
    tokens.push_back(s.substr(0, pos));
    s.erase(0, pos + delimiter.length());
  }
  tokens.push_back(s);

  return tokens;
}
