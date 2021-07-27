//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer_test.cpp
//
// Identification: test/buffer/leaderboard_test.cpp
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"

namespace bustub {

TEST(LeaderboardTest, Time) {
  DiskManagerMemory *dm = new DiskManagerMemory();
  BufferPoolManager *bpm = new BufferPoolManager(1000000, dm);
  page_id_t temp;
  for (int i = 0; i < 1000000; i++) {
    bpm->NewPage(&temp);
  }
  for (int i = 0; i < 1000000; i--) {
    bpm->UnpinPage(i, false);
    bpm->FetchPage(i);
    bpm->UnpinPage(i, false);
  }
  for (int i = 999999; i >= 0; i--) {
    bpm->DeletePage(i);
    bpm->NewPage(&temp);
    bpm->UnpinPage(temp, false);
    bpm->DeletePage(temp);
    bpm->NewPage(&temp);
  }
  delete bpm;
  delete dm;
}
}  // namespace bustub
