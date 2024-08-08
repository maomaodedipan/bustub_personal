//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // throw NotImplementedException("DiskExtendibleHashTable is not implemented");
    BasicPageGuard header_guard = bpm_->NewPageGuarded(&header_page_id_);
    auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    header->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  if (header_page_id_ == INVALID_PAGE_ID){
    return false;
  }
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t hash_key = Hash(key);
  uint32_t dir_id = header_page->HashToDirectoryIndex(hash_key);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_id);
  header_guard.Drop();
  if (dir_page_id == INVALID_PAGE_ID){
    return false;
  }

  ReadPageGuard dir_page_guard = bpm_->FetchPageRead(dir_page_id);
  auto dir_page = dir_page_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash_key);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  dir_page_guard.Drop();
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  ReadPageGuard bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K,V,KC>>();
  V value;
  bool lookup_success = bucket_page->Lookup(key,value, cmp_);
  if (lookup_success) {
    result->push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint32_t dir_idx = header->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = header->GetDirectoryPageId(dir_idx);
  if (dir_page_id == INVALID_PAGE_ID) {
      return InsertToNewDirectory(header, dir_idx, hash, key, value);
  }
  header_guard.Drop();

  auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto htable_dir = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();

  uint32_t bucket_idx = htable_dir->HashToBucketIndex(hash);
  page_id_t bucket_page_id = htable_dir->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
      return InsertToNewBucket(htable_dir, bucket_idx, key, value);
  }

  auto bucket_page = bpm_->FetchPageWrite(bucket_page_id);
  auto htable_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetDataMut());
  
  if (!htable_bucket->IsFull()) {
      return htable_bucket->Insert(key, value, cmp_);
  }

  // if the bucket is full, we should split the bucket first
  while (htable_bucket->IsFull()) {
      uint32_t local_depth = htable_dir->GetLocalDepth(bucket_idx);
      uint32_t global_depth = htable_dir->GetGlobalDepth();
      if (local_depth == global_depth && global_depth == directory_max_depth_) {
          return false;
      }
      // 1.create a new bucket
      page_id_t new_bucket_page_id = INVALID_PAGE_ID;
      auto new_page = bpm_->NewPageGuarded(&new_bucket_page_id);
      if (new_bucket_page_id == INVALID_PAGE_ID) {
          return false;
      }
      auto new_bucket_page = new_page.UpgradeWrite();
      auto new_htable_bucket = new_bucket_page.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      new_htable_bucket->Init(bucket_max_size_);
      // 2.migrate entries to the new bucket
      MigrateEntries(htable_bucket, new_htable_bucket, 0, htable_dir->GetLocalDepthMask(bucket_idx));
      // 3.compare local depth with global depth
      if (local_depth + 1 > global_depth) {
          // local depth + 1 is larger than global depth
          htable_dir->IncrGlobalDepth();
      }
      // 4.update dir mapping with split image
      // example:
      // bucket_idx   **101
      //              *1101 *0101
      // local = 3, global = 5, dis = 2, need to update 4 bucket
      global_depth = htable_dir->GetGlobalDepth();
      uint32_t update_count = 1 << (global_depth - local_depth);
      uint32_t base_idx = bucket_idx & htable_dir->GetLocalDepthMask(bucket_idx);
      bucket_idx = htable_dir->HashToBucketIndex(hash);
      for (uint32_t i = 0; i < update_count; ++i) {
          uint32_t tmp_idx = (i << local_depth) + base_idx;
          if (i % 2 == 0) {
              htable_dir->IncrLocalDepth(tmp_idx);
          } else {
              if (tmp_idx == bucket_idx) {
                  htable_bucket = new_htable_bucket;
              }
              UpdateDirectoryMapping(htable_dir, tmp_idx, new_bucket_page_id, local_depth + 1, 0);
          }
      }
  }

  return htable_bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
 page_id_t new_dir_page_id = INVALID_PAGE_ID;
 auto new_dir_page_basic_guard = bpm_->NewPageGuarded(&new_dir_page_id);
 auto new_dir_page_write_guard = new_dir_page_basic_guard.UpgradeWrite();
 auto new_dir_page = new_dir_page_write_guard.AsMut<ExtendibleHTableDirectoryPage>();

 new_dir_page->Init(directory_max_depth_);
 header->SetDirectoryPageId(directory_idx,new_dir_page_id);
 uint32_t bucket_index = new_dir_page->HashToBucketIndex(hash);
 return InsertToNewBucket(new_dir_page,bucket_index,key,value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_page_basic_guard = bpm_->NewPageGuarded(&new_bucket_page_id);
  auto new_bucket_page_write_guard = new_bucket_page_basic_guard.UpgradeWrite();
  auto new_bucket_page = new_bucket_page_write_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

  new_bucket_page->Init(bucket_max_size_);  
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
  return new_bucket_page->Insert(key,value,cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  directory->SetLocalDepth(new_bucket_idx,  new_local_depth);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                        ExtendibleHTableBucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,
                                                        uint32_t local_depth_mask) {
  ++local_depth_mask;
  for (uint32_t i = 0; i < old_bucket->Size(); ++i) {
      K key = old_bucket->KeyAt(i);
      uint32_t hash = Hash(key);
      if ((hash & local_depth_mask) != 0) {
          auto &entry = old_bucket->EntryAt(i);
          new_bucket->Insert(entry.first, entry.second, cmp_);
          old_bucket->RemoveAt(i);
      }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  auto header_page = bpm_->FetchPageWrite(header_page_id_);
  auto htable_header = reinterpret_cast<ExtendibleHTableHeaderPage *>(header_page.GetDataMut());
  uint32_t dir_idx = htable_header->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = htable_header->GetDirectoryPageId(dir_idx);
  if (dir_page_id == -1) {
      return false;
  }
  header_page.Drop();
  auto dir_page = bpm_->FetchPageWrite(dir_page_id);
  auto htable_dir = reinterpret_cast<ExtendibleHTableDirectoryPage *>(dir_page.GetDataMut());
  uint32_t bucket_idx = htable_dir->HashToBucketIndex(hash);
  page_id_t bucket_page_id = htable_dir->GetBucketPageId(bucket_idx);
  if (bucket_page_id == -1) {
      return false;
  }
  auto bucket_page = bpm_->FetchPageWrite(bucket_page_id);
  auto htable_bucket = reinterpret_cast<ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetDataMut());
  bool res = htable_bucket->Remove(key, cmp_);
  bucket_page.Drop();
  if (!res) {
      return false;
  }
  auto check_page_id = bucket_page_id;
  auto check_page = bpm_->FetchPageRead(check_page_id);
  auto check_bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(check_page.GetData());
  uint32_t local_depth = htable_dir->GetLocalDepth(bucket_idx);
  uint32_t global_depth = htable_dir->GetGlobalDepth();
  while (local_depth > 0) {
      uint32_t convert_mask = 1 << (local_depth - 1);
      uint32_t merge_bucket_idx = bucket_idx ^ convert_mask;
      uint32_t merge_local_depth = htable_dir->GetLocalDepth(merge_bucket_idx);
      page_id_t merge_bucket_page_id = htable_dir->GetBucketPageId(merge_bucket_idx);
      auto merge_bucket_page = bpm_->FetchPageRead(merge_bucket_page_id);
      auto merge_bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(merge_bucket_page.GetData());
      if (merge_local_depth != local_depth || (!check_bucket->IsEmpty() && !merge_bucket->IsEmpty())) {
          break;
      }
      if (check_bucket->IsEmpty()) {
          check_page.Drop();
          bpm_->DeletePage(check_page_id);
          check_bucket = merge_bucket;
          check_page_id = merge_bucket_page_id;
          check_page = std::move(merge_bucket_page);
      } else {
          merge_bucket_page.Drop();
          bpm_->DeletePage(merge_bucket_page_id);
      }
      htable_dir->DecrLocalDepth(bucket_idx);
      local_depth = htable_dir->GetLocalDepth(bucket_idx);
      uint32_t local_depth_mask = htable_dir->GetLocalDepthMask(bucket_idx);
      uint32_t mask_idx = bucket_idx & local_depth_mask;
      uint32_t update_count = 1 << (global_depth - local_depth);
      for (uint32_t i = 0; i < update_count; ++i) {
          uint32_t tmp_idx = (i << local_depth) + mask_idx;
          UpdateDirectoryMapping(htable_dir, tmp_idx, check_page_id, local_depth, 0);
      }
  }
  while (htable_dir->CanShrink()) {
      htable_dir->DecrGlobalDepth();
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
