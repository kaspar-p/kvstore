#pragma once

#include <cstdint>
#include <memory>
#include <optional>

struct PageId;
using DataRef = const PageId;

class Evictor {
 public:
  virtual ~Evictor() = default;

  /**
   * @brief Insert a new entry into the cache. Marks the entry as used, that is,
   * InsertOne() followed by MarkUsed() of the same page does nothing. The
   * return value is std::nullopt if there was no page to evict, and if present,
   * is the page evicted by this insert.
   *
   * For an LRU, it would place the new page at the beginning of the queue, and
   * for Clock eviction, it would place it wherever the header pointer is,
   * evicting a the page if there is one.
   *
   * @param page The page to put into the eviction algorithm.
   *
   * @return std::optional<std::shared_ptr<BufferedPage>> The page evicted if
   * present, std::nullopt if not.
   */
  virtual std::optional<DataRef> Insert(DataRef page) = 0;

  /**
   * @brief Mark a page as dirty/used if it gets accessed.
   *
   * This is used for prioritization. For example, with an LRU algorithm it
   * would place the node at the front of the queue when marked. For Clock
   * eviction, it would just mark that node dirty.
   *
   * Does nothing if the page does not exist.
   *
   * @param page The page that was accessed.
   */
  virtual void MarkUsed(DataRef page) = 0;

  /**
   * @brief Resize the eviction data to now have @param n elements
   */
  virtual void Resize(uint32_t n) = 0;
};

class ClockEvictor : public Evictor {
 private:
  class ClockEvictorImpl;
  std::unique_ptr<ClockEvictorImpl> impl_;

 public:
  ClockEvictor();
  ~ClockEvictor() override;

  std::optional<DataRef> Insert(DataRef page) override;
  void MarkUsed(DataRef page) override;
  void Resize(uint32_t n) override;
};
