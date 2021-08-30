/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(Page *page, int index, BufferPoolManager *bpm)
    : page_(page), index_(index), bpm_(bpm) {
  leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  page_->RUnlatch();
  bpm_->UnpinPage(leaf_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ == leaf_->GetSize(); }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return leaf_->GetItem(index_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (++index_ == leaf_->GetSize() && leaf_->GetNextPageId() != INVALID_PAGE_ID) {
    Page *next_page = bpm_->FetchPage(leaf_->GetNextPageId());
    next_page->RLatch();
    page_->RUnlatch();
    bpm_->UnpinPage(page_->GetPageId(), false);
    page_ = next_page;
    leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page_->GetData());
    index_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
