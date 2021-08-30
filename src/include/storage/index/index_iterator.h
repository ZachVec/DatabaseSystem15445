//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  IndexIterator(Page *page, int index, BufferPoolManager *bpm);
  IndexIterator();
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const { return leaf_ == itr.leaf_ && index_ == itr.index_; }

  bool operator!=(const IndexIterator &itr) const { return !this->operator==(itr); }

 private:
  Page *page_;
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_;
  int index_;
  BufferPoolManager *bpm_;
};

}  // namespace bustub
