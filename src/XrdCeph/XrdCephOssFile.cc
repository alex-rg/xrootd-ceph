//------------------------------------------------------------------------------
// Copyright (c) 2014-2015 by European Organization for Nuclear Research (CERN)
// Author: Sebastien Ponce <sebastien.ponce@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#include <sys/types.h>
#include <unistd.h>

#include <vector>

#include "XrdCeph/XrdCephPosix.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"

#include "XrdCeph/XrdCephOssFile.hh"
#include "XrdCeph/XrdCephOss.hh"

extern XrdSysError XrdCephEroute;

unsigned int g_ReadVBufSize = DEF_READV_BUFFER_SIZE;

XrdCephOssFile::XrdCephOssFile(XrdCephOss *cephOss) : m_fd(-1), m_cephOss(cephOss) {}

int XrdCephOssFile::Open(const char *path, int flags, mode_t mode, XrdOucEnv &env) {
  try {
    int rc = ceph_posix_open(&env, path, flags, mode);
    if (rc < 0) return rc;
    m_fd = rc;
    return XrdOssOK;
  } catch (std::exception &e) {
    XrdCephEroute.Say("open : invalid syntax in file parameters");
    return -EINVAL;
  }
}

int XrdCephOssFile::Close(long long *retsz) {
  return ceph_posix_close(m_fd);
}

ssize_t XrdCephOssFile::Read(off_t offset, size_t blen) {
  return XrdOssOK;
}

ssize_t XrdCephOssFile::Read(void *buff, off_t offset, size_t blen) {
  return ceph_posix_pread(m_fd, buff, blen, offset);
}

static void aioReadCallback(XrdSfsAio *aiop, size_t rc) {
  aiop->Result = rc;
  aiop->doneRead();
}

int XrdCephOssFile::Read(XrdSfsAio *aiop) {
  return ceph_aio_read(m_fd, aiop, aioReadCallback);
}

ssize_t XrdCephOssFile::ReadRaw(void *buff, off_t offset, size_t blen) {
  return Read(buff, offset, blen);
}

ssize_t XrdCephOssFile::process_block(off_t block_start, size_t block_len, std::vector<int> chunks_to_read, XrdOucIOVec *readV) {
  char *ptr, *buf;
  ssize_t data_read, real_data_read;
  data_read = 0;
  if (chunks_to_read.size() == 1) {
    int idx = chunks_to_read[0];
    data_read = ceph_async_read(m_fd, (void*)readV[idx].data, readV[idx].size, readV[idx].offset);
  } else {
    try {
      buf = new char[g_ReadVBufSize];
    } catch(std::bad_alloc&) {
       XrdCephEroute.Say("Can not allocate memory for readv buffer! Exiting\n");
       return -ENOMEM;
    }
    real_data_read = ceph_async_read(m_fd, (void*) buf, block_len, block_start);
    if (real_data_read < (ssize_t)block_len) {
      char errmsg[100];
      snprintf(errmsg, 100, "Expected %lu bytes, got %ld. Exiting\n", block_len, real_data_read);
      XrdCephEroute.Say(errmsg);
      delete[] buf;
      return -ESPIPE;
    }
    for (int i: chunks_to_read) {
      ptr = buf;
      ptr += readV[i].offset - block_start;
      memcpy(readV[i].data, ptr, readV[i].size);
      data_read += readV[i].size;
    }
    delete[] buf;
  }
  return data_read;
}

ssize_t XrdCephOssFile::ReadV(XrdOucIOVec *readV, int n) {
  ssize_t data_read, block_data_read, block_start, block_len, cur_end, new_end;
  ssize_t new_block_start, new_block_len;
  std::vector<int> chunks_to_read;
  int ceph_read_count = 0;
  block_start = -1;
  block_len = 0;
  data_read = 0;
  for (int i = 0; i < n; i++) {
    //Calculate new block borders, after a chunk will be added to the block
    //To do so first calculate end of current block and end of chunk
    cur_end = block_start + block_len - 1;
    new_end = readV[i].offset + readV[i].size - 1;

    //Update block start if new chunk's start is preced it.
    if (readV[i].offset < block_start or block_start < 0) {
      new_block_start = readV[i].offset;
    } else {
      new_block_start = block_start;
    }

    //Update block end if chunk's end is greater than the block's one
    if (cur_end > new_end) {
      new_end = cur_end;
    }

    //Now we know updated block's start and end, let's update its lengt
    new_block_len = new_end - new_block_start + 1;

    if (new_block_len > g_ReadVBufSize && block_len > 0) {
        block_data_read = process_block(block_start, block_len, chunks_to_read, readV);
        if (block_data_read > 0) {
          data_read += block_data_read;
          ceph_read_count++;
        } else {
          char errmsg[100];
          snprintf(errmsg, 100, "Error while reading block at %ld, got %ld\n", block_start, block_data_read);
          XrdCephEroute.Say(errmsg);
          return block_data_read;
        }
        chunks_to_read.clear();
        block_start = readV[i].offset;
        block_len = readV[i].size;
    } else {
      block_start = new_block_start;
      block_len = new_block_len;
    }
    chunks_to_read.push_back(i);
  }

  //Extract chunks from the last block
  if (chunks_to_read.size() > 0) {
    block_data_read = process_block(block_start, block_len, chunks_to_read, readV);
    if (block_data_read > 0) {
      data_read += block_data_read;
      ceph_read_count++;
    } else {
      char errmsg[100];
      snprintf(errmsg, 100, "Error while reading block at %ld, got %ld\n", block_start, block_data_read);
      XrdCephEroute.Say(errmsg);
      return block_data_read;
    }
  }

  return data_read;
}


int XrdCephOssFile::Fstat(struct stat *buff) {
  return ceph_posix_fstat(m_fd, buff);
}

ssize_t XrdCephOssFile::Write(const void *buff, off_t offset, size_t blen) {
  return ceph_posix_pwrite(m_fd, buff, blen, offset);
}

static void aioWriteCallback(XrdSfsAio *aiop, size_t rc) {
  aiop->Result = rc;
  aiop->doneWrite();
}

int XrdCephOssFile::Write(XrdSfsAio *aiop) {
  return ceph_aio_write(m_fd, aiop, aioWriteCallback);
}

int XrdCephOssFile::Fsync() {
  return ceph_posix_fsync(m_fd);
}

int XrdCephOssFile::Ftruncate(unsigned long long len) {
  return ceph_posix_ftruncate(m_fd, len);
}
