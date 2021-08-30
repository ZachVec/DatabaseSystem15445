//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  root_latch_.lock();
  if (IsEmpty()) {
    root_latch_.unlock();
    return false;
  }
  ValueType value;
  Page *page = FindLeafPage(key);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  if (leaf->Lookup(key, &value, comparator_)) {
    result->emplace_back(std::move(value));
    RUnlatchAndUnpin(page);
    return true;
  }
  RUnlatchAndUnpin(page);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  root_latch_.lock();
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    StartNewTree(key, value);
    WUnlatchAndUnpin(transaction, true);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw std::runtime_error("StartNewTree: out of memory");
  }
  LeafPage *root = reinterpret_cast<LeafPage *>(page->GetData());
  root->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  UpdateRootPageId();
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  Page *page = FindLeafPageWLatch(key, transaction, Operation::INSERT);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  // if already exists
  if (leaf->Lookup(key, nullptr, comparator_)) {
    WUnlatchAndUnpin(transaction, false);
    return false;
  }
  // otherwise insert, and if reach the max size after insert
  if (leaf->Insert(key, value, comparator_) == leaf->GetMaxSize()) {
    LeafPage *sibl = Split<LeafPage>(leaf);
    InsertIntoParent(leaf, sibl->KeyAt(0), sibl, transaction);
    buffer_pool_manager_->UnpinPage(sibl->GetPageId(), true);
  }
  WUnlatchAndUnpin(transaction, true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw std::runtime_error("Split: out of memory");
  }
  N *ret = reinterpret_cast<N *>(page->GetData());
  if (node->IsLeafPage()) {
    LeafPage *old_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_node = reinterpret_cast<LeafPage *>(ret);
    new_node->Init(page_id, old_node->GetParentPageId(), leaf_max_size_);
    new_node->SetNextPageId(old_node->GetNextPageId());
    old_node->SetNextPageId(new_node->GetPageId());
    old_node->MoveHalfTo(new_node);
  } else {
    InternalPage *old_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_node = reinterpret_cast<InternalPage *>(ret);
    new_node->Init(page_id, old_node->GetParentPageId(), internal_max_size_);
    old_node->MoveHalfTo(new_node, buffer_pool_manager_);
  }
  return ret;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw std::runtime_error("Out Of Memory");
    }
    InternalPage *root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(root_page_id_);
    old_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId();
  } else {
    Page *page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    if (page == nullptr) {
      throw std::runtime_error("Out Of Memory");
    }
    InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
    if (parent->GetSize() == parent->GetMaxSize()) {       // if split
      InternalPage *siblin = Split<InternalPage>(parent);  // then split
      if (parent->GetSize() == parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId())) {
        new_node->SetParentPageId(siblin->GetPageId());
        siblin->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        siblin->MoveFirstToEndOf(parent, siblin->KeyAt(0), buffer_pool_manager_);
      }
      InsertIntoParent(parent, siblin->KeyAt(0), siblin, transaction);
      buffer_pool_manager_->UnpinPage(siblin->GetPageId(), true);
    } else {
      parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.lock();
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    WUnlatchAndUnpin(transaction, false);
    return;
  }
  Page *page = FindLeafPageWLatch(key, transaction, Operation::REMOVE);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  int size = leaf->RemoveAndDeleteRecord(key, comparator_);
  if (size < leaf->GetMinSize() && CoalesceOrRedistribute(leaf, transaction)) {
    WUnlatchAndUnpin(transaction, true);
    DeletePages(transaction);
  } else {
    WUnlatchAndUnpin(transaction, true);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target node should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    if (AdjustRoot(node)) {
      transaction->AddIntoDeletedPageSet(node->GetPageId());
      return true;
    }
    return false;
  }
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  Page *sibling_page = FetchPageAndWLatch(parent->ValueAt(index == 0 ? 1 : index - 1));
  N *sibling = reinterpret_cast<N *>(sibling_page->GetData());
  if (node->IsLeafPage() && node->GetSize() + sibling->GetSize() >= node->GetMaxSize()) {  // Leaf Redistribute
    Redistribute(sibling, node, index);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }
  if (!node->IsLeafPage() && node->GetSize() + sibling->GetSize() > node->GetMaxSize()) {  // Internal Redistribute
    Redistribute(sibling, node, index);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    sibling_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
    return false;
  }
  // If not able to redistribute, merge instead.
  Coalesce(&sibling, &node, &parent, index, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  sibling_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  if (index == 0) {
    std::swap(*neighbor_node, *node);
    index = 1;
  }
  if ((*node)->IsLeafPage()) {
    LeafPage *leaf = *reinterpret_cast<LeafPage **>(node);
    LeafPage *sibl = *reinterpret_cast<LeafPage **>(neighbor_node);
    leaf->MoveAllTo(sibl);
  } else {
    InternalPage *inode = *reinterpret_cast<InternalPage **>(node);
    InternalPage *sibli = *reinterpret_cast<InternalPage **>(neighbor_node);
    inode->MoveAllTo(sibli, (*parent)->KeyAt(index), buffer_pool_manager_);
  }
  transaction->AddIntoDeletedPageSet((*node)->GetPageId());
  (*parent)->Remove(index);
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    return CoalesceOrRedistribute(*parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  Page *page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  InternalPage *parent = reinterpret_cast<InternalPage *>(page->GetData());
  if (node->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(node);
    LeafPage *sibl = reinterpret_cast<LeafPage *>(neighbor_node);
    if (index == 0) {
      sibl->MoveFirstToEndOf(leaf);
      parent->SetKeyAt(1, sibl->KeyAt(0));
    } else {
      sibl->MoveLastToFrontOf(leaf);
      parent->SetKeyAt(index, leaf->KeyAt(0));
    }
  } else {
    InternalPage *inode = reinterpret_cast<InternalPage *>(node);
    InternalPage *sibli = reinterpret_cast<InternalPage *>(neighbor_node);
    if (index == 0) {
      sibli->MoveFirstToEndOf(inode, parent->KeyAt(1), buffer_pool_manager_);
      parent->SetKeyAt(1, sibli->KeyAt(0));
    } else {
      sibli->MoveLastToFrontOf(inode, parent->KeyAt(index), buffer_pool_manager_);
      parent->SetKeyAt(index, sibli->KeyAt(0));
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {  // Case #1
    InternalPage *old_root = reinterpret_cast<InternalPage *>(old_root_node);
    Page *new_root_page = buffer_pool_manager_->FetchPage(old_root->RemoveAndReturnOnlyChild());
    BPlusTreePage *new_root = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root_page->GetPageId();
    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    UpdateRootPageId();
    return true;
  }
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  return INDEXITERATOR_TYPE(FindLeafPage(KeyType(), true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *page = FindLeafPage(key);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  return INDEXITERATOR_TYPE(page, leaf->KeyIndex(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() {
  Page *page = FindLeafPage(KeyType(), true);
  LeafPage *leaf = reinterpret_cast<LeafPage *>(page->GetData());
  for (Page *next; leaf->GetNextPageId() != INVALID_PAGE_ID; page = next) {
    next = FetchPageAndRLatch(leaf->GetNextPageId());
    RUnlatchAndUnpin(page);
    leaf = reinterpret_cast<LeafPage *>(next->GetData());
  }
  return INDEXITERATOR_TYPE(page, leaf->GetSize(), buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // Assuming that root_latch_ has been held, and tree is not empty
  Page *page = FetchPageAndRLatch(root_page_id_);
  root_latch_.unlock();
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  for (Page *child; !node->IsLeafPage(); page = child) {
    InternalPage *tmp = reinterpret_cast<InternalPage *>(node);
    child = FetchPageAndRLatch(leftMost ? tmp->ValueAt(0) : tmp->Lookup(key, comparator_));
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    node = reinterpret_cast<BPlusTreePage *>(child->GetData());
  }
  return page;
}

/* Concurrent Access Helper Functions */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPageWLatch(const KeyType &key, Transaction *transaction, Operation operation) {
  // Assuming that root_latch_ has been held, and tree is not empty
  Page *page = FetchPageAndWLatch(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!node->IsLeafPage()) {
    InternalPage *tmp = reinterpret_cast<InternalPage *>(node);
    if (node->IsSafe(operation)) {
      WUnlatchAndUnpin(transaction, false);
    }
    transaction->AddIntoPageSet(page);
    page = FetchPageAndWLatch(tmp->Lookup(key, comparator_));
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  if (node->IsSafe(operation)) {
    WUnlatchAndUnpin(transaction, false);
  }
  transaction->AddIntoPageSet(page);
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::WUnlatchAndUnpin(Transaction *transaction, bool is_dirty) {
  auto &pages = *transaction->GetPageSet();
  for (const auto &page : pages) {
    if (page == nullptr) {
      root_latch_.unlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), is_dirty);
    }
  }
  pages.clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePages(Transaction *transaction) {
  auto &page_ids = *transaction->GetDeletedPageSet();
  for (const auto &page_id : page_ids) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  page_ids.clear();
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FetchPageAndWLatch(const page_id_t &page_id) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("Out Of Memory");
  }
  page->WLatch();
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FetchPageAndRLatch(const page_id_t &page_id) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw std::runtime_error("Out Of Memory");
  }
  page->RLatch();
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RUnlatchAndUnpin(Page *page) {
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
