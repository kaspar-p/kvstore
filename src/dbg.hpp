#pragma once

#include <cstdint>
#include <string>

/**
 * @brief Repeat `input` a `num` amount of times
 */
std::string repeat(const std::string& input, uint32_t num);

/**
 * @brief Returns the top `bits` bits of `num`. The most significant!
 */
std::string bit_string(uint32_t num, uint32_t bits);
uint8_t prefix_bit(uint32_t prefix, uint32_t bit_offset);
std::vector<std::string> split_string(std::string s, std::string&& delimiter);
