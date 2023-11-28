#include "minheap.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"

bool sortByKey(const std::pair<K, int> &a, const std::pair<K, int> &b) {
  return a.first < b.first;
}

class MinHeap::MinHeapImpl {
 private:
  std::vector<std::pair<K, int>> heap;  // each key and its run number

  int GetSmallerIndex(int current_index, int new_index) {
    if (new_index < this->heap.size() &&
        this->heap.at(new_index).first < this->heap.at(current_index).first) {
      return new_index;
    }

    // Assume current_index is always within vector
    if (this->heap.at(current_index).first < this->heap.at(new_index).first) {
      return current_index;
    }

    // If the keys are the same, return the newer version
    if (this->heap.at(new_index).first == this->heap.at(current_index).first &&
        this->heap.at(new_index).second > this->heap.at(current_index).second) {
      return new_index;
    }
    return current_index;
  }

  void HeapifyDown(int index) {
    int left = 2 * index + 1;
    int right = 2 * index + 2;
    int smallest = index;

    smallest = GetSmallerIndex(smallest, left);
    smallest = GetSmallerIndex(smallest, right);

    if (smallest != index) {
      std::swap(this->heap.at(index), this->heap.at(smallest));
      this->HeapifyDown(smallest);
    }
  }

 public:
  explicit MinHeapImpl(std::vector<K> initial_keys) {
    this->heap.resize(initial_keys.size());
    for (int i = 0; i < this->heap.size(); i++) {
      this->heap.at(i) = std::make_pair(initial_keys[i], i);
    }
    std::sort(this->heap.begin(), this->heap.end(), sortByKey);
  }
  ~MinHeapImpl() = default;

  std::optional<std::pair<K, int>> Extract() {
    if (this->heap.empty()) {
      return std::nullopt;
    }

    std::pair<K, int> min = heap.at(0);
    this->heap.at(0) = this->heap.back();
    this->heap.pop_back();

    if (this->heap.size() > 1) {
      this->HeapifyDown(0);
    }

    return min;
  }

  std::optional<std::pair<K, int>> InsertAndExtract(
      std::pair<K, int> next_pair) {
    if (this->heap.empty()) {
      heap.push_back(next_pair);
      return std::nullopt;
    }

    std::pair<K, int> previous_min = this->heap.at(0);
    this->heap.at(0) = next_pair;
    this->HeapifyDown(0);
    return previous_min;
  }

  bool IsEmpty() { return this->heap.empty(); }
};

MinHeap::MinHeap(const std::vector<K> &initial_keys)
    : impl(std::make_unique<MinHeapImpl>(initial_keys)) {}

MinHeap::~MinHeap() = default;

bool MinHeap::IsEmpty() { return this->impl->IsEmpty(); }

std::optional<std::pair<K, int>> MinHeap::Extract() {
  return this->impl->Extract();
}

std::optional<std::pair<K, int>> MinHeap::InsertAndExtract(
    std::pair<K, int> next_pair) {
  return this->impl->InsertAndExtract(next_pair);
}
