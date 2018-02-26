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

#include "dist/replication/meta_server/duplication/duplication_info.h"

namespace dsn {
namespace replication {

error_code duplication_info::do_alter_status(duplication_status::type to)
{
    if (_is_altering) {
        return ERR_BUSY;
    }

    if (status == duplication_status::DS_REMOVED || status == duplication_status::DS_INIT) {
        return ERR_OBJECT_NOT_FOUND;
    }

    dassert(status != duplication_status::DS_INIT, "state transition from DS_INIT");

    if (to == duplication_status::DS_INIT) {
        return ERR_INVALID_PARAMETERS;
    }

    if (status != to) {
        _is_altering = true;
        next_status = to;
    }

    // if status == to, return OK.
    return ERR_OK;
}

} // namespace replication
} // namespace dsn
