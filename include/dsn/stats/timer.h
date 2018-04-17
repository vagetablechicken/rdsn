// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

// Copyright (c) 2010 Baidu, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstdint>
#include <chrono>

namespace dsn {
namespace stats {

class timer
{
public:
    // Start this timer
    void start()
    {
        _start = std::chrono::system_clock::now();
        _stop = _start;
    }

    // Stop this timer
    void stop() { _stop = std::chrono::system_clock::now(); }

    // Get the elapse from start() to stop(), in various units.
    std::chrono::nanoseconds n_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(_stop - _start);
    }
    std::chrono::microseconds u_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(n_elapsed());
    }
    std::chrono::milliseconds m_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(n_elapsed());
    }
    std::chrono::seconds s_elapsed() const
    {
        return std::chrono::duration_cast<std::chrono::seconds>(n_elapsed());
    }

private:
    std::chrono::system_clock::time_point _stop;
    std::chrono::system_clock::time_point _start;
};

} // namespace stats
} // namespace dsn
