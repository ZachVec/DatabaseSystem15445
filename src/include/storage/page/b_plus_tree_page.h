//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <cassert>
#include <climits>
#include <cstdlib>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/index/generic_key.h"

namespace bustub {
class ScopedPage {
 public:
  ScopedPage(page_id_t page_id, BufferPoolManager *bpm) : page_id_(page_id), bpm_(bpm) {
    page_ = bpm->FetchPage(page_id);
  }
  ScopedPage(Page *page, BufferPoolManager *bpm) : page_id_(page->GetPageId()), bpm_(bpm) { page_ = page; }
  void SetDirty() { is_dirty_ = true; }
  void SetDelete() { is_delete_ = true; }
  Page *GetPage() { return page_; }
  void UnpinAndPin(page_id_t page_id) {
    bpm_->UnpinPage(page_id_, is_dirty_);
    if (is_delete_) {
      assert(bpm_->DeletePage(page_id_));
    }
    is_dirty_ = is_delete_ = false;
    page_ = bpm_->FetchPage(page_id_ = page_id);
  }
  ~ScopedPage() {
    bpm_->UnpinPage(page_id_, is_dirty_);
    if (is_delete_) {
      assert(bpm_->DeletePage(page_id_));
    }
  }

 private:
  Page *page_ = nullptr;
  page_id_t page_id_;
  BufferPoolManager *bpm_;
  bool is_dirty_ = false;
  bool is_delete_ = false;
};

#define MappingType std::pair<KeyType, ValueType>

#define INDEX_TEMPLATE_ARGUMENTS template <typename KeyType, typename ValueType, typename KeyComparator>

// define page type enum
enum class IndexPageType { INVALID_INDEX_PAGE = 0, LEAF_PAGE, INTERNAL_PAGE };

/**
 * Both internal and leaf page are inherited from this page.
 *
 * It actually serves as a header part for each B+ tree page and
 * contains information shared by both leaf page and internal page.
 *
 * Header format (size in byte, 24 bytes in total):
 * ----------------------------------------------------------------------------
 * | PageType (4) | LSN (4) | CurrentSize (4) | MaxSize (4) |
 * ----------------------------------------------------------------------------
 * | ParentPageId (4) | PageId(4) |
 * ----------------------------------------------------------------------------
 */
class BPlusTreePage {
 public:
  bool IsLeafPage() const;
  bool IsRootPage() const;
  void SetPageType(IndexPageType page_type);

  int GetSize() const;
  void SetSize(int size);
  void IncreaseSize(int amount);

  int GetMaxSize() const;
  void SetMaxSize(int max_size);
  int GetMinSize() const;

  page_id_t GetParentPageId() const;
  void SetParentPageId(page_id_t parent_page_id);

  page_id_t GetPageId() const;
  void SetPageId(page_id_t page_id);

  void SetLSN(lsn_t lsn = INVALID_LSN);

 private:
  // member variable, attributes that both internal and leaf page share
  IndexPageType page_type_ __attribute__((__unused__));
  lsn_t lsn_ __attribute__((__unused__));
  int size_ __attribute__((__unused__));
  int max_size_ __attribute__((__unused__));
  page_id_t parent_page_id_ __attribute__((__unused__));
  page_id_t page_id_ __attribute__((__unused__));
};

}  // namespace bustub
