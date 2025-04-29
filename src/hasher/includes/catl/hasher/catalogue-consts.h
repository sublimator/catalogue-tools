#pragma once

#include "catl/common/catalogue-types.h"
#include "catl/v1/catl-v1-structs.h"

// Use the common types
using CATLHeader = catl::common::CATLHeader;
using LedgerInfoV1 = catl::v1::LedgerInfo;

// Alias the constants for backward compatibility
static constexpr uint32_t CATL = catl::common::CATL_MAGIC;
static constexpr uint16_t CATALOGUE_VERSION_MASK =
    catl::common::CATALOGUE_VERSION_MASK;
static constexpr uint16_t CATALOGUE_COMPRESS_LEVEL_MASK =
    catl::common::CATALOGUE_COMPRESS_LEVEL_MASK;
