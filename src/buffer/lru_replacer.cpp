//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : capcity(num_pages) {
  this->head = new Node;
  this->tail = new Node;
  this->head->next = tail;
  this->head->prev = tail;
  this->tail->next = head;
  this->tail->prev = head;
}

LRUReplacer::~LRUReplacer() {
  for (pNode curr = this->head->next, next; curr != this->tail; curr = next) {
    next = curr->next;
    curr->prev = nullptr;
    curr->next = nullptr;
    delete curr;
  }
  this->head->next = nullptr;
  this->head->prev = nullptr;
  this->tail->next = nullptr;
  this->tail->prev = nullptr;
  delete this->head;
  delete this->tail;
}

/**
 * @brief Remove LRU object compare to other elements tracked by Replacer.
 *
 * @param frame_id output parameter
 * @return true if the replacer ain't empty, remove the LRU object and store its contents in @param frame_id.
 * @return false otherwise.
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock(this->latch);
  if (mps.empty()) {
    frame_id = nullptr;
    return false;
  }
  pNode node = this->tail->prev;
  *frame_id = rmNode(node);
  mps.erase(*frame_id);
  return true;
}

/**
 * @brief This method should be called after a page is pinned in the BufferPoolManager.
 * It should remove the frame containing the pinned page from the LRUReplacer.
 *
 * @param frame_id id of the pinned page
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock(this->latch);
  auto it = this->mps.find(frame_id);
  if (it != this->mps.end()) {
    rmNode(it->second);
    mps.erase(it);
  }
}

/**
 * @brief This method should be called when the pin_count of a page becomes 0.
 * This method should add the frame containing the unpinned page to the LRUReplacer.
 *
 * @param frame_id id of the unpinned page
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock(this->latch);
  if (mps.size() >= this->capcity || mps.find(frame_id) != mps.end()) {
    return;
  }
  mps[frame_id] = mkNode(frame_id);
}

/**
 * @brief This method returns the number of frames that are currently in the LRUReplacer.
 *
 * @return size_t # of frames that are currently in the LRUReplacer
 */
size_t LRUReplacer::Size() { return mps.size(); }

frame_id_t LRUReplacer::rmNode(pNode node) {
  frame_id_t frame_id = node->frame_id;
  node->prev->next = node->next;
  node->next->prev = node->prev;
  node->prev = nullptr;
  node->next = nullptr;
  delete node;
  return frame_id;
}

LRUReplacer::pNode LRUReplacer::mkNode(frame_id_t frame_id) {
  pNode node = new Node;
  node->frame_id = frame_id;
  node->next = this->head->next;
  node->prev = this->head;
  this->head->next->prev = node;
  this->head->next = node;
  return node;
}

}  // namespace bustub
