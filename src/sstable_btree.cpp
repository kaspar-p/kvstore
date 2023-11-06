#include <fstream>
#include <optional>
#include <utility>
#include <vector>
#include <queue>
#include <iostream>

#include "sstable.hpp"

#include "constants.hpp"
#include "memtable.hpp"

struct LeafNode {
    uint32_t magic_number;
    uint32_t garbage;
    std::vector<std::pair<uint64_t, uint64_t>> kv_pairs;
    uint64_t right_leaf_block_ptr;
};

struct InternalNode {
    uint32_t magic_number;
    uint32_t num_children;
    uint64_t last_child_block_ptr;
    std::vector<std::pair<uint64_t, uint64_t>> child_ptrs; // key, child ptr
};

struct sstable_btree_node {
    uint64_t offset;
    uint64_t global_max;
};

void SstableBTree::Flush(std::fstream& file, MemTable& memtable)
{
  // questions
  // pages? what does this mean
  // what if there are more key/val than possible to fit in a sst? what is the sst max size?
  assert(file.is_open());
  assert(file.good());
  std::vector<std::pair<K, V>> pairs = memtable.ScanAll();

  file.seekp(0); 
  assert(file.good());
  
  int order = floor(kPageSize / 16);
  std::vector<uint64_t> wbuf; // makeshift solution
  wbuf.push_back(0x00db00beef00db00); // magic number
  wbuf.push_back(pairs.size()); // # key value pairs in the file
  wbuf.push_back(0x0000000000000000); // dummy root block ptr
  
  for (int i = 3; i < kPageSize; i++) {
    wbuf.push_back(0); // pad the rest of the page with zeroes
  }

  std::queue<sstable_btree_node> creation_queue;

  // create and write leaf nodes
  int i = 0;
  while (i < pairs.size()) {
    // info node used for the queue
    sstable_btree_node info_node;
    info_node.offset = i * kPageSize;

    LeafNode leaf_node;
    // magic number + garbage (4 + 4 bytes)
    leaf_node.magic_number = 0x00db0011;
    leaf_node.garbage = 0x00000000;

    for (int j = 0; j < order; j++) {
      if (i < pairs.size()) {
        // key value pairs (8 + 8 bytes)
        leaf_node.kv_pairs.push_back(pairs[i]);
        // set global max for info node using the last/rightmost node in the leaf
        if (j == order-1 or i == pairs.size() - 1) {
          info_node.global_max = pairs[i].first;
          creation_queue.push(info_node);
        }
        i++;
      } else {
        // TODO: do i need to pad these zeroes?
        // pad the rest of the page with zeroes
        leaf_node.kv_pairs.push_back(std::make_pair(0, 0));
      }
    }
    // right leaf block ptr (8 ptr)
    // - don't need for the last element
    // pointer to the right leaf node
    // offset from the beginning of the file
    if (i < pairs.size()) {
      leaf_node.right_leaf_block_ptr = i/order * kPageSize;
    } else {
      leaf_node.right_leaf_block_ptr = BLOCK_NULL;
    }
    // Convert the LeafNode struct to bytes and append to wbuf
    char* data = reinterpret_cast<char*>(&leaf_node);
    for (size_t k = 0; k < sizeof(LeafNode); k++) {
        wbuf.push_back(data[k]);
    }
    // Pad the rest of the page with zeroes
    for (int k = 0; k < kPageSize - sizeof(LeafNode); k++) {
      wbuf.push_back(0);
    }
  }

  // start creating internal nodes
  // note that the address of the first internal node is the address of the last leaf node + 1
  // since this is offsets we can calculate this using the length of the queue at the start of the algorithim
  while (creation_queue.size() > 1) {
    int queue_length = creation_queue.size();
    int start_offset = creation_queue.back().offset + kPageSize;
    int i = 0;

    while (i < queue_length) {
      // info node used for the queue
      sstable_btree_node info_node;
      info_node.offset = start_offset + i * kPageSize;

      // if i = last don't make internal node

      // create internal node
      InternalNode internal_node;
      // magic number + garbage (4 + 4 bytes)
      internal_node.magic_number = 0x00db00ff;
      internal_node.num_children = 0;
      // last child block ptr (8 bytes)

      for (int j; j < order; j++) {
        if (i < queue_length) {
          sstable_btree_node child = creation_queue.front();
          creation_queue.pop();
          if (j == order-1) {
            // last child
            // TODO: what if not enought elements to fill up to order-1?
            internal_node.last_child_block_ptr = child.offset;
            info_node.global_max = child.global_max; // TODO: applies here too
            creation_queue.push(info_node); // TODO: applies here too
          } else {
            // key value pairs (8 + 8 bytes)
            internal_node.child_ptrs.push_back(std::make_pair(child.global_max, child.offset));
            // set global max for info node
          }
          internal_node.num_children++;
          i++;
        } else {
          break;
        }
      }

      // Convert the InternalNode struct to bytes and append to wbuf
      uint8_t* data = reinterpret_cast<uint8_t*>(&internal_node);
      for (size_t k = 0; k < sizeof(InternalNode); k++) {
          wbuf.push_back(data[k]);
      }

      // Pad the rest of the page with zeroes
      for (int k = 0; k < kPageSize - sizeof(InternalNode); k++) {
        wbuf.push_back(0);
      }
    }
  }
  // the last node in the queue is the parent
  sstable_btree_node root = creation_queue.front();
  creation_queue.pop();
  wbuf[2] = root.offset; // update dummy root block ptr to actual root block ptr

  // write wbuf to file, page size is kPageSize
  for (int i = 0; i < wbuf.size(); i++) {
    file.write(reinterpret_cast<char*>(&wbuf[i]), sizeof(kPageSize));
    assert(file.good());
  }

  file.flush();
  assert(file.good());
};

std::optional<V> SstableBTree::GetFromFile(std::fstream& file, const K key)
{
  // std::vector<u_int64_t> buf;
  // assert(file.is_open());
  // assert(file.good());

  // file.seekg(0);

  // // file.read(reinterpret_cast<u_int64_t>(buf), kPageSize); // TODO: why doesn't this work
  // assert(file.good());

  // if (buf[0] != 0x00db00beef00db00) {
  //   std::cout << "Magic number wrong! Expected " << 0x00db00beef00db00 << " but got "
  //             << buf[0] << '\n';
  //   exit(1);
  // }

  

  // // If there are no elements
  // int elems = buf[];
  // if (elems == 0) {
  //   return std::nullopt;
  // }
  
  (void) file;
  (void) key;
  return std::nullopt;
};

std::vector<std::pair<K, V>> SstableBTree::ScanInFile(std::fstream& file,
                                                      const K lower,
                                                      const K upper)
{
  (void)file;
  (void)lower;
  (void)upper;
  std::vector<std::pair<K, V>> l {};
  return l;
};
