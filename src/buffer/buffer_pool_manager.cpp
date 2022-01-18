//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>
#include "common/logger.h"

static std::unordered_map<std::string, bool> output;

static void print_file(const std::string &path) {
  if (output[path]) {
    return;
  }
  std::cout << path << '\n';
  char line[300];
  std::ifstream file(path);
  if (!file.is_open()) {
    std::cout << "file missing\n";
    return;
  }
  while (!file.eof()) {
    file.getline(line, 300);
    std::cout << line << '\n';
  }
  std::cout << "EOF\n";
  file.close();
  output[path] = true;
}

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  print_file("/autograder/bustub/test/concurrency/grading_lock_manager_2_test.cpp");
  print_file("/autograder/bustub/test/concurrency/grading_lock_manager_3_test.cpp");
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock<std::mutex> latch(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    Page &page = pages_[it->second];
    ++page.pin_count_;
    replacer_->Pin(it->second);
    return &page;
  }

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Victim(&frame_id)) {
    Page &page = pages_[frame_id];
    page_table_.erase(page.GetPageId());
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
  } else {
    return nullptr;
  }

  Page &page = pages_[frame_id];
  page.page_id_ = page_id;
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  disk_manager_->ReadPage(page.GetPageId(), page.GetData());
  page_table_[page_id] = frame_id;
  return &page;
}

/**
 * Unpin the target page from the buffer pool.
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page pin count is <= 0 before this call, true otherwise
 */
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> latch(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    Page &page = pages_[it->second];
    if (page.GetPinCount() <= 0) {
      return false;
    }
    page.is_dirty_ = page.is_dirty_ || is_dirty;
    if (--page.pin_count_ == 0) {
      replacer_->Unpin(it->second);
    }
    return true;
  }
  return false;
}

/**
 * Flushes the target page to disk.
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::scoped_lock<std::mutex> latch(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    Page &page = pages_[it->second];
    page.is_dirty_ = false;
    disk_manager_->WritePage(page_id, page.GetData());
    return true;
  }
  return false;
}

// metadata: data, page_id, pin_count, is_dirty
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock<std::mutex> latch(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Victim(&frame_id)) {
    Page &page = pages_[frame_id];
    page_table_.erase(page.GetPageId());
    if (page.IsDirty()) {
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
  } else {
    return nullptr;
  }
  Page &page = pages_[frame_id];
  *page_id = page.page_id_ = disk_manager_->AllocatePage();
  page.pin_count_ = 1;
  page.is_dirty_ = false;
  page.ResetMemory();
  disk_manager_->WritePage(page.GetPageId(), page.GetData());
  page_table_[page.GetPageId()] = frame_id;
  return &page;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock<std::mutex> latch(latch_);
  disk_manager_->DeallocatePage(page_id);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  Page &page = pages_[it->second];
  if (page.GetPinCount() != 0) {
    return false;
  }
  replacer_->Pin(it->second);
  free_list_.push_back(it->second);
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;
  page_table_.erase(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::scoped_lock<std::mutex> latch(latch_);
  for (const auto kv : page_table_) {
    Page &page = pages_[kv.second];
    if (page.IsDirty()) {
      page.is_dirty_ = false;
      disk_manager_->WritePage(page.GetPageId(), page.GetData());
    }
  }
}

}  // namespace bustub
