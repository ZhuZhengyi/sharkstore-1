// Copyright (c) 2018 The SharkStore Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "statistics.h"

#include <inttypes.h>

namespace sharkstore {
namespace monitor {

const char *HistogramTypeName(HistogramType type) {
    switch (type) {
        case HistogramType::kQWait:
            return "QWait";
        case HistogramType::kDeal:
            return "Deal";
        case HistogramType::kStore:
            return "Store";
        case HistogramType::kRaft:
            return "Raft";
        default:
            return "<unknown>";
    }
}

void Statistics::PushTime(HistogramType type, uint64_t time) {
    histograms_[static_cast<uint32_t>(type)].Add(time);
}

void Statistics::GetData(HistogramType type, HistogramData *data) {
    std::lock_guard<std::mutex> lock(aggregate_lock_);
    histograms_[static_cast<uint32_t>(type)].Data(data);
}

std::string Statistics::ToString(HistogramType type) const {
    std::lock_guard<std::mutex> lock(aggregate_lock_);
    return histograms_[static_cast<uint32_t>(type)].ToString();
}

std::string Statistics::ToString() const {
    std::string result;

    std::lock_guard<std::mutex> lock(aggregate_lock_);
    for (uint32_t i = 0; i < kHistogramTypeNum; ++i) {
        auto &h = histograms_[i];
        if (h.num() == 0) continue;

        char buffer[200] = {'\0'};
        HistogramData data;
        h.Data(&data);
        snprintf(
                buffer, 200,
                "%s statistics => count: %" PRIu64 "  P50: %f  P95: %f  P99: %f  Max: %f\n",
                HistogramTypeName(static_cast<HistogramType>(i)),
                h.num(), data.median, data.percentile95, data.percentile99, data.max);
        result.append(buffer);
    }

    return result;
}

void Statistics::Reset() {
    std::lock_guard<std::mutex> lock(aggregate_lock_);
    for (auto &h : histograms_) {
        h.Clear();
    }
}


}  // namespace monitor
}  // namespace sharkstore
