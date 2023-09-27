#include "memtable.hpp"

typedef int K;
typedef std::string V;

class KvStore
{
private:
  std::unique_ptr<MemTable<K, V>> memtable;

public:
  /**
   * @brief Opens a database, either new or used.
   * The data directory is where all persistent data files
   * for the key-value store will be. Permissions issues
   * will throw errors.
   *
   * See `Close()` for closing.
   *
   * @param data_dir The path to the data directory.
   */
  void Open(std::string data_dir);

  /**
   * @brief Paired operation with `Open()`, closes the
   * database and cleans up resources.
   */
  void Close();

  std::vector<std::pair<K, V>> Scan(K lower, K upper);
  V Get(const K key);
  void Put(const K key, const V value);
};