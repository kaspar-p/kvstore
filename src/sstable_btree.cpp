#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "constants.hpp"
#include "sstable.hpp"

struct LeafNode {
  uint32_t magic_number{};
  uint32_t garbage{};
  std::vector<std::pair<uint64_t, uint64_t>> kv_pairs{};
  uint64_t right_leaf_block_ptr{};
};

struct InternalNode {
  uint32_t magic_number{};
  uint32_t num_children{};
  uint64_t last_child_block_ptr{};
  std::vector<std::pair<uint64_t, uint64_t>> child_ptrs;  // key, child ptr
};

struct SstableBtreeNode {
  uint64_t offset;
  uint64_t global_max;
};

SstableBTree::SstableBTree() = default;

K SstableBTree::GetMinimum(std::fstream& file) const { return 0; }
K SstableBTree::GetMaximum(std::fstream& file) const { return 1; }

void SstableBTree::Flush(std::fstream& file,
                         std::vector<std::pair<K, V>>& pairs) const {
  // questions
  // pages? what does this mean
  // what if there are more key/val than possible to fit in a sst? what is the
  // sst max size?
  assert(file.is_open());
  assert(file.good());

  file.seekp(0);
  assert(file.good());

  uint64_t order = floor(kPageSize / 16) - 1;
  std::vector<uint64_t> wbuf;
  wbuf.push_back(0x00db00beef00db00);  // magic number
  wbuf.push_back(0x0000000000000001);
  wbuf.push_back(pairs.size());  // # key value pairs in the file
  wbuf.push_back(0);             // dummy root block ptr

  for (int i = 4; i < kPageSize / sizeof(uint64_t); i++) {
    wbuf.push_back(0x0000000000000000);  // pad the rest of the page with zeroes
  }
  std::queue<SstableBtreeNode> creation_queue;

  // create and write leaf nodes
  uint64_t i = 0;
  while (i < pairs.size()) {
    // info node used for the queue
    SstableBtreeNode info_node = {
        .offset = (1 + i / order) * kPageSize,
        .global_max = 0,
    };
    
    LeafNode leaf_node;
    // magic number + garbage (4 + 4 bytes)
    leaf_node.magic_number = 0x00db0011;
    leaf_node.garbage = 0x00000000;

    for (int j = 0; j < order; j++) {
      if (i < pairs.size()) {
        // key value pairs (8 + 8 bytes)
        leaf_node.kv_pairs.push_back(pairs.at(i));
        // set global max for info node using the last/rightmost node in the
        // leaf
        if (j == order - 1 or i == pairs.size() - 1) {
          info_node.global_max = pairs.at(i).first;
          creation_queue.push(info_node);
        }
        i++;
      } else {
        break;
      }
    }
    // right leaf block ptr (8 ptr)
    if (i < pairs.size()) {
      leaf_node.right_leaf_block_ptr = (1 + i / order) * kPageSize;
    } else {
      leaf_node.right_leaf_block_ptr = BLOCK_NULL;
    }

    const size_t actual_size = 2 + leaf_node.kv_pairs.size() * 2;
    wbuf.push_back(static_cast<uint64_t>(leaf_node.magic_number) << 32 |
                   static_cast<uint64_t>(leaf_node.garbage));
    wbuf.push_back(leaf_node.right_leaf_block_ptr);
    for (int j = 0; j < leaf_node.kv_pairs.size(); j++) {
      wbuf.push_back(leaf_node.kv_pairs[j].first);
      wbuf.push_back(leaf_node.kv_pairs[j].second);
    }

    // Pad the rest of the page with zeroes
    for (size_t k = actual_size; k < kPageSize / sizeof(uint64_t); k++) {
      wbuf.push_back(0);
    }
  }

  // start creating internal nodes
  // note that the address of the first internal node is the address of the last
  // leaf node + 1 since this is offsets we can calculate this using the length
  // of the queue at the start of the algorithm
  
  while (creation_queue.size() > 1) {
    uint64_t queue_length = creation_queue.size();
    uint64_t start_offset = creation_queue.back().offset + kPageSize;

    int i = 0;
    
    while (i < queue_length) {
      // info node used for the queue
      SstableBtreeNode info_node;
      info_node.offset = start_offset + i/order * kPageSize;
      // if i = last don't make internal node

      // create internal node
      InternalNode internal_node;
      // magic number + garbage (4 + 4 bytes)
      internal_node.magic_number = 0x00db00ff;
      internal_node.num_children = 0;
      // last child block ptr (8 bytes)

      for (int j = 0; j < order; j++) {
        if (i < queue_length) {
          SstableBtreeNode child = creation_queue.front();
          creation_queue.pop();
          if (j == order - 1 or i == queue_length - 1) {
            // last child
            // TODO: what if not enought elements to fill up to order-1?
            internal_node.last_child_block_ptr = child.offset;
            info_node.global_max = child.global_max;  // applies here too
            creation_queue.push(info_node);           // applies here too
          } else {
            // key value pairs (8 + 8 bytes)
            internal_node.child_ptrs.push_back(
                std::make_pair(child.global_max, child.offset));
            // set global max for info node
          }
          internal_node.num_children++;
          i++;
        } else {
          break;
        }
      }

      const size_t actual_size = 2 + internal_node.child_ptrs.size() * 2;
      wbuf.push_back(static_cast<uint64_t>(internal_node.magic_number) << 32 |
                     static_cast<uint64_t>(internal_node.num_children));
      wbuf.push_back(internal_node.last_child_block_ptr);
      for (int j = 0; j < internal_node.child_ptrs.size(); j++) {
        wbuf.push_back(internal_node.child_ptrs[j].first);
        wbuf.push_back(internal_node.child_ptrs[j].second);
      }

      // Pad the rest of the page with zeroes
      for (int k = actual_size; k < kPageSize / sizeof(uint64_t); k++) {
        wbuf.push_back(0);
      }
      // TODO: child ptr is BLOCK_NULL if there isn't a leaf node, in
      // documentation??
    }
  }
  // the last node in the queue is the parent
  SstableBtreeNode root = creation_queue.front();
  creation_queue.pop();
  wbuf[3] =
      root.offset;  // update dummy root block ptr to actual root block ptr

  for (uint64_t& elem : wbuf) {
    file.write(reinterpret_cast<char*>(&elem), sizeof(uint64_t));
    assert(file.good());
  }

  file.flush();
  assert(file.good());
};

std::optional<V> SstableBTree::GetFromFile(std::fstream& file,
                                           const K key) const {
  uint64_t buf[kPageSize];
  assert(file.is_open());
  assert(file.good());
  
  file.seekg(0);
  file.read(reinterpret_cast<char*>(buf), kPageSize);
  assert(file.good());

  if (buf[0] != 0x00db00beef00db00) {
    std::cout << "Magic number wrong! Expected " << 0x00db00beef00db00
              << " but got " << buf[0] << '\n';
    exit(1);
  }

  // if there are no elements
  int elems = buf[2];
  if (elems == 0) {
    return std::nullopt;
  }

  uint64_t cur_offset = buf[3];  // meta block size + root block ptr
  std::cout << "cur_offset:" << cur_offset << std::endl;
  bool leaf_node = false;
  while (!leaf_node) {
    file.seekg(cur_offset);
    file.read(reinterpret_cast<char*>(buf), kPageSize);
    assert(file.good());
    
    int header_size = 2;
    int pair_size = 2;

    if ((buf[0] >> 32) == 0x00db0011) {  // leaf node
      leaf_node = true;
      // binary search to find which key to return
      // if key and cant find then return std::nullopt
      // if key and can find then return val
      int left = header_size;
      int right = header_size + (kPageSize - 16) / 16 * pair_size;

      // check right node ptr to see if the leaf node is the rightmost one =>
      // potential not full.
      if (buf[1] == 0xffffffffffffffff &&
          elems % ((kPageSize - 16) / 16) == 0) {
        right = header_size + ((kPageSize - 16) / 16) * pair_size;
      } else if (buf[1] == 0xffffffffffffffff) {
        right = header_size + elems % ((kPageSize - 16) / 16) * pair_size;
      }
      int mid = left + floor((right - left) / 4) * 2;
      
      while (left <= right) {
        int mid = left + floor((right - left) / 4) * 2;
        if (buf[mid] == key) {
          return std::make_optional(buf[mid + 1]);
        }

        if (buf[mid] < key) {
          left = mid + 2;
        } else {
          right = mid - 2;
        }
      }
      return std::nullopt;

    } else if ((buf[0] >> 32) == 0x00db00ff) {  // internal node
      uint32_t num_children = buf[0] & 0x00000000ffffffff;
      int left = header_size;
      int right = header_size + (num_children-1) * pair_size;
      std::cout << "4" << buf[4] << '\n';
      if (key <= buf[left]) {
        cur_offset = buf[left + 1];
      } else if (key > buf[right-2]) {
        cur_offset = buf[1];
      } else {
        int mid = left + floor((right - left) / 4) * 2;
        std::cout << "mid:" << mid << std::endl;
        std::cout << "buf[mid]:" << buf[mid-2] << std::endl;
        while (left <= right) {
          mid = left + floor((right - left) / 4) * 2;
          if (buf[mid-2] < key && key <= buf[mid]) {
            std::cout << "miasdfasd:" << mid << std::endl;
            cur_offset = buf[mid + 1];
            break;
          } else if (buf[mid] < key) {
            left = mid + 2;
          } else {
            right = mid - 2;
          }
        }
      }

    } else {
      std::cout << "Magic number wrong! Expected " << 0x00db0011 << " or "
                << 0x00db00ff << " but got " << (buf[0] >> 32) << '\n';
      exit(1);
    }
  }
  return std::nullopt;
};

std::vector<std::pair<K, V>> SstableBTree::ScanInFile(std::fstream& file,
                                                      const K lower,
                                                      const K upper) const {
  uint64_t buf[kPageSize];
  assert(file.is_open());
  assert(file.good());

  file.seekg(0);
  file.read(reinterpret_cast<char*>(buf), kPageSize);
  assert(file.good());

  if (buf[0] != 0x00db00beef00db00) {
    std::cout << "Magic number wrong! Expected " << 0x00db00beef00db00
              << " but got " << buf[0] << '\n';
    exit(1);
  }

  std::vector<std::pair<K, V>> l;

  int elems = buf[2];
  if (elems == 0) {
    return l;
  }

  uint64_t cur_offset = buf[3];  // meta block size + root block ptr
  bool leaf_node = false;
  int mid = 0;
  while (!leaf_node) {
    file.seekg(cur_offset);
    file.read(reinterpret_cast<char*>(buf), kPageSize);
    assert(file.good());

    int header_size = 2;
    int pair_size = 2;

    if ((buf[0] >> 32) == 0x00db0011) {  // leaf node
      leaf_node = true;
      // binary search to find which key to return
      // if key and cant find then return std::nullopt
      // if key and can find then return val
      int left = header_size;
      int right = header_size + (kPageSize - 16) / 16 * pair_size;

      // check right node ptr to see if the leaf node is the rightmost one =>
      // potential not full.
      if (buf[1] == 0xffffffffffffffff &&
          elems % ((kPageSize - 16) / 16) == 0) {
        right = header_size + ((kPageSize - 16) / 16) * pair_size;
      } else if (buf[1] == 0xffffffffffffffff) {
        right = header_size + elems % ((kPageSize - 16) / 16) * pair_size;
      }
      right -= pair_size;
      while (left <= right) {
        mid = left + floor((right - left) / 4) * 2;
        if (left == right || buf[mid] == lower) {
          break;
        }

        if (buf[mid] < lower) {
          left = mid + pair_size;
        } else {
          right = mid - pair_size;
        }
      }
      // assert(buf[mid] >= lower);
      if (buf[mid] < lower) {
        return l;
      }

    } else if ((buf[0] >> 32) == 0x00db00ff) {  // internal node
      uint32_t num_children = buf[0] & 0x00000000ffffffff;
      int left = header_size;
      int right = header_size + (num_children-1) * pair_size;
      if (lower <= buf[left]) {
        cur_offset = buf[left + 1];
      } else if (lower > buf[right-2]) {
        cur_offset = buf[1];
      } else {
        while (left <= right) {
          int mid = left + floor((right - left) / 4) * 2;
          if (buf[mid-2] < lower && lower <= buf[mid]) {
            cur_offset = buf[mid + 1];
            break;
          } else if (buf[mid] < lower) {
            left = mid + 2;
          } else {
            right = mid - 2;
          }
        }
      }

    } else {
      std::cout << "Magic number wrong! Expected " << 0x00db0011 << " or "
                << 0x00db00ff << " but got " << (buf[0] >> 32) << '\n';
      exit(1);
    }
  }
  int walk = mid;
  while ((buf[0] >> 32 == 0x00db0011) && buf[walk] <= upper &&
         walk < kPageSize / sizeof(uint64_t)) {
    if (buf[1] == 0xffffffffffffffff &&
        elems % ((kPageSize - 16) / 16) != 0 &&
        walk >= 2 + (elems % ((kPageSize-16)/16)) * 2) {
      std::cout << "elems:" << elems << std::endl;
      std::cout << "break" << std::endl;
      break;
    }; 
    l.push_back(std::make_pair(buf[walk], buf[walk + 1]));
    walk += 2;

    if (walk == kPageSize / sizeof(uint64_t) && buf[1] != 0xffffffffffffffff) {
      file.seekg(buf[1]);
      file.read(reinterpret_cast<char*>(buf), kPageSize);
      assert(file.good());
      walk = 2;
    }
  }
  return l;
};
