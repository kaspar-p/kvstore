#include "minheap.hpp"

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constants.hpp"

std::vector<std::pair<K, V>> minheap_merge(
    std::vector<std::vector<std::pair<K, V>>> &sorted_buffers) {
  std::vector<std::pair<K, V>> result{};

  std::vector<std::vector<std::pair<K, V>>> nonempty_buffers;
  std::vector<K> initial_keys{};
  std::vector<size_t> cursors{};

  for (auto const &buf : sorted_buffers) {
    if (buf.size() > 0) {
      nonempty_buffers.push_back(buf);
      initial_keys.push_back(buf.at(0).first);
      cursors.push_back(0);
    }
  }

  MinHeap heap(initial_keys);

  std::optional<K> prev_key = std::nullopt;
  std::optional<std::pair<K, size_t>> min;

  while (!heap.IsEmpty()) {
    min = heap.Extract();
    size_t buffer_idx = min->second;

    if (!prev_key.has_value() || prev_key != min->first) {
      int cursor_idx = cursors.at(buffer_idx);
      std::vector<std::pair<K, V>> buffer = nonempty_buffers.at(buffer_idx);
      auto pair = buffer.at(cursor_idx);
      result.push_back(pair);
    }

    cursors.at(buffer_idx)++;

    if (cursors.at(buffer_idx) < nonempty_buffers.at(buffer_idx).size()) {
      std::pair<K, V> next =
          nonempty_buffers.at(buffer_idx).at(cursors.at(buffer_idx));
      heap.Insert(std::make_pair(next.first, buffer_idx));
    }

    prev_key = min->first;
  }

  return result;
}

bool sortByKey(const std::pair<K, int> &a, const std::pair<K, int> &b) {
  if (a.first == b.first) {
    return a.second > b.second;
  }
  return a.first < b.first;
}

class MinHeap::MinHeapImpl {
 private:
  std::vector<std::pair<K, std::size_t>> heap;  // each key and its run number

  std::size_t GetSmallerIndex(std::size_t current_index,
                              std::size_t new_index) {
    // Assume current_index is always within vector, but new_index may not be
    if (new_index >= this->heap.size()) {
      return current_index;
    }

    if (this->heap.at(current_index).first < this->heap.at(new_index).first) {
      return current_index;
    }

    if (this->heap.at(new_index).first < this->heap.at(current_index).first) {
      return new_index;
    }

    // If the keys are the same, return the newer version
    if (this->heap.at(new_index).second > this->heap.at(current_index).second) {
      return new_index;
    }
    return current_index;
  }

  void HeapifyUp(int index) {
    if (index == 0) {
      return;
    }
    int parent = (index - 1) / 2;
    int smallest = index;

    smallest = GetSmallerIndex(smallest, parent);

    if (smallest == index) {
      std::swap(this->heap.at(index), this->heap.at(parent));
      this->HeapifyUp(parent);
    }
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
    for (std::size_t i = 0; i < this->heap.size(); i++) {
      this->heap.at(i) = std::make_pair(initial_keys.at(i), i);
    }
    std::sort(this->heap.begin(), this->heap.end(), sortByKey);
  }
  ~MinHeapImpl() = default;

  void Insert(std::pair<K, int> new_pair) {
    this->heap.emplace_back(new_pair);
    if (this->heap.size() > 1) {
      this->HeapifyUp(this->heap.size() - 1);
    }
  }

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
      heap.emplace_back(next_pair);
      return std::nullopt;
    }

    std::pair<K, int> previous_min = this->heap.at(0);
    if (next_pair.first < previous_min.first) {
      return next_pair;
    }
    if (next_pair.first == previous_min.first &&
        next_pair.second > previous_min.second) {
      return next_pair;
    }
    this->heap.at(0) = next_pair;
    this->HeapifyDown(0);
    return previous_min;
  }

  bool IsEmpty() const { return this->heap.empty(); }
  std::size_t Size() const { return this->heap.size(); }
};

MinHeap::MinHeap(const std::vector<K> &initial_keys)
    : impl(std::make_unique<MinHeapImpl>(initial_keys)) {}
MinHeap::~MinHeap() = default;
bool MinHeap::IsEmpty() const { return this->impl->IsEmpty(); }
std::optional<std::pair<K, std::size_t>> MinHeap::Extract() {
  return this->impl->Extract();
}
std::size_t MinHeap::Size() const { return this->impl->Size(); }
std::optional<std::pair<K, std::size_t>> MinHeap::InsertAndExtract(
    std::pair<K, std::size_t> next_pair) {
  return this->impl->InsertAndExtract(next_pair);
}
void MinHeap::Insert(std::pair<K, std::size_t> next_pair) {
  return this->impl->Insert(next_pair);
}
