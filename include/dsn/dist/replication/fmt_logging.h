/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <fmt/format.h>

// The macros below no longer use the default snprintf method for log message formatting,
// instead we use fmt::format.
// TODO(wutao1): prevent contruction of std::string for each log.

#define dinfo_f(...) dinfo(fmt::format(__VA_ARGS__).c_str())
#define ddebug_f(...) ddebug(fmt::format(__VA_ARGS__).c_str())
#define dwarn_f(...) dwarn(fmt::format(__VA_ARGS__).c_str())
#define derror_f(...) derror(fmt::format(__VA_ARGS__).c_str())
#define dfatal_f(...) dfatal(fmt::format(__VA_ARGS__).c_str())
#define dassert_f(x, ...) dassert(x, fmt::format(__VA_ARGS__).c_str())