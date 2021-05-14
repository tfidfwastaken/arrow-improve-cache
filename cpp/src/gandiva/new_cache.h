#pragma once

#include <list>
#include <unordered_map>
#include <utility>

#include "arrow/util/optional.h"

// modified from boost LRU cache -> the boost cache supported only an
// ordered map.
namespace gandiva {
// a cache which evicts the least recently used item when it is full
template <class Key, class Value>
class NewCache {
 public:
  using key_type = Key;
  using value_type = Value;
  using list_type = std::list<key_type>;
  struct hasher {
    template <typename I>
    std::size_t operator()(const I& i) const {
      return i.Hash();
    }
  };
  using map_type =
      std::unordered_map<key_type, std::pair<value_type, typename list_type::iterator>,
                         hasher>;

  explicit NewCache(size_t capacity, size_t threshold)
      : cache_capacity_(capacity), threshold_(threshold) {}

  ~NewCache() {}

  size_t size() const { return map_.size(); }

  size_t capacity() const { return cache_capacity_; }

  bool empty() const { return map_.empty(); }

  bool contains(const key_type& key) { return map_.find(key) != map_.end(); }

  void insert(const key_type& key, const value_type& value) {
    typename map_type::iterator i = map_.find(key);
    if (i == map_.end()) {
      // insert item into the cache, but first check if it is full
      if (size() >= cache_capacity_) {
        // cache is full, evict the least recently used item
        evict();
      }

      // insert the new item
      lru_list_.push_front(key);
      map_[key] = std::make_pair(value, lru_list_.begin());

      if(size() == threshold_) {
        // if threshold is reached, form common subexpr
        // build all projectors as needed using llvm generator
      }
    }
  }

  arrow::util::optional<value_type> get(const key_type& key) {
    // lookup value in the cache
    typename map_type::iterator value_for_key = map_.find(key);
    if (value_for_key == map_.end()) {
      // value not in cache
      return arrow::util::nullopt;
    }

    // return the value, but first update its place in the most
    // recently used list
    typename list_type::iterator position_in_lru_list = value_for_key->second.second;
    if (position_in_lru_list != lru_list_.begin()) {
      // move item to the front of the most recently used list
      lru_list_.erase(position_in_lru_list);
      lru_list_.push_front(key);

      // update iterator in map
      position_in_lru_list = lru_list_.begin();
      const value_type& value = value_for_key->second.first;
      map_[key] = std::make_pair(value, position_in_lru_list);

      // return the value
      return value;
    } else {
      // the item is already at the front of the most recently
      // used list so just return it
      return value_for_key->second.first;
    }
  }

  void clear() {
    map_.clear();
    lru_list_.clear();
  }

 private:
  void evict() {
    // evict item from the end of most recently used list
    typename list_type::iterator i = --lru_list_.end();
    map_.erase(*i);
    lru_list_.erase(i);
  }

 private:
  map_type map_;
  list_type lru_list_;
  size_t cache_capacity_;
  size_t threshold_;
};
}  // namespace gandiva
