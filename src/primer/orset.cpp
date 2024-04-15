#include "primer/orset.h"
#include <algorithm>
#include <string>
#include <vector>
#include "common/exception.h"
#include "fmt/format.h"

namespace bustub {

template <typename T>
auto ORSet<T>::Contains(const T &elem) const -> bool {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Contains is not implemented");
  bool result = std::any_of(elements_.begin(), elements_.end(), [&elem](std::pair<T, uid_t> item) {
        return item.first==elem;
    });
  return result;
}

template <typename T>
void ORSet<T>::Add(const T &elem, uid_t uid) {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Add is not implemented");
  
  // add
  elements_.insert(std::make_pair(elem, uid));

  // erase
  std::set<std::pair<T, uid_t>> result;
  std::set_difference(elements_.begin(), elements_.end(), tombstones_.begin(), tombstones_.end(), 
                        std::inserter(result, result.begin()));
  elements_ = result;
  // std::sort(elements_.begin(),elements_.end());
}

template <typename T>
void ORSet<T>::Remove(const T &elem) {
  // TODO(student): Implement this
  //throw NotImplementedException("ORSet<T>::Remove is not implemented");
  bool result = std::any_of(elements_.begin(), elements_.end(), [&elem](std::pair<T, uid_t> item) {
        return item.first==elem;
    });

  if(result){
    auto it = std::find_if(elements_.begin(), elements_.end(),[&elem](const std::pair<T, uid_t> pair){
      return pair.first == elem;
    });

    if (it != elements_.end()) {
        tombstones_.insert(*it);
        elements_.erase(it); // Remove the pair from the set
    } 
  }
}

template <typename T>
void ORSet<T>::Merge(const ORSet<T> &other) {
  // TODO(student): Implement this
  // throw NotImplementedException("ORSet<T>::Merge is not implemented");
  std::set<std::pair<T, uid_t>>  difference1;
  std::set<std::pair<T, uid_t>>  difference2;
  std::set<std::pair<T, uid_t>>  merged2;
  std::set_difference(elements_.begin(), elements_.end(), other.tombstones_.begin(), other.tombstones_.end(), 
                      std::inserter(difference1, difference1.begin()));
  std::set_difference(other.elements_.begin(), other.elements_.end(), tombstones_.begin(), tombstones_.end(), 
                      std::inserter(difference2, difference2.begin()));
  std::set_union(difference1.begin(), difference1.end(), difference2.begin(), difference2.end(),
                   std::inserter(merged2, merged2.begin()));
  elements_ = merged2;
  std::set<std::pair<T, uid_t>>  merged;
  std::set_union(tombstones_.begin(), tombstones_.end(), other.tombstones_.begin(), other.tombstones_.end(),
                   std::inserter(merged, merged.begin()));
  tombstones_ = merged;
}

template <typename T>
auto ORSet<T>::Elements() const -> std::vector<T> {
  // TODO(student): Implement this
  //throw NotImplementedException("ORSet<T>::Elements is not implemented");
  std::vector<T> result;
    for (const std::pair<T, uid_t>& elem : elements_) {
        result.push_back(elem.first);
    }
    return result;
}

template <typename T>
auto ORSet<T>::ToString() const -> std::string {
  auto elements = Elements();
  std::sort(elements.begin(), elements.end());
  return fmt::format("{{{}}}", fmt::join(elements, ", "));
}

template class ORSet<int>;
template class ORSet<std::string>;

}  // namespace bustub
