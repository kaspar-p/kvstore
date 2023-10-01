#include <chrono>
#include <iostream>
#include <string>

#include "kvstore.hpp"

int main()
{
  KvStore db {};

  using std::chrono::duration;
  using std::chrono::duration_cast;
  using std::chrono::high_resolution_clock;
  using std::chrono::microseconds;

  auto t1 = high_resolution_clock::now();

  Options options;
  options.overwrite = true;
  db.Open("kvstore.db", options);

  for (long i = 0; i < 4097; i++) {
    db.Put(i, 100 * i);
  }

  auto t2 = high_resolution_clock::now();

  K query = 2435;
  const std::optional<V> v = db.Get(query);

  auto t3 = high_resolution_clock::now();
  auto ms1_int = duration_cast<microseconds>(t2 - t1);
  auto ms2_int = duration_cast<microseconds>(t3 - t2);

  std::cout << "QUERY: " << query << "\n"
            << "GOT: " << v.value() << "\n"
            << "TOOK (setup): " << ms1_int.count() << "\n"
            << "TOOK (query): " << ms2_int.count() << std::endl;

  return 0;
}
