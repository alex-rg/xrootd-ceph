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

#define READV_BUFFER_SIZE 16777216

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
  char * buf = new char[READV_BUFFER_SIZE];
  char *ptr;
  ssize_t data_read, real_data_read;
  data_read = 0;
  printf("processing block %lu %lu\n", block_start, block_len);
  if (chunks_to_read.size() == 1) {
    int idx = chunks_to_read[0];
    data_read = ceph_async_read(m_fd, (void*)readV[idx].data, readV[idx].size, readV[idx].offset);
  } else {
    real_data_read = ceph_async_read(m_fd, (void*) buf, block_len, block_start);
    printf("read %ld bytes\n", real_data_read);
    if (real_data_read < (ssize_t)block_len) {
      //log("Requested %lu bytes, got %lu!\n", block_len, data_read);
      return -data_read;
    }
    for (int i: chunks_to_read) {
      ptr = buf;
      ptr += readV[i].offset - block_start;
      memcpy(readV[i].data, ptr, readV[i].size);
      data_read += readV[i].size;
      printf("Extracting block %d, data read %ld\n",i,  data_read);
    }
  }
  delete[] buf;
  printf("returning  %ld\n", data_read);
  return data_read;
}

ssize_t XrdCephOssFile::ReadV(XrdOucIOVec *readV, int n) {
  ssize_t data_read, chunks_data_read, block_start, block_len, tlen;
  std::vector<int> chunks_to_read;
  block_start = -1;
  block_len = 0;
  data_read = 0;
  printf("starting readv\n");
  for (int i = 0; i < n; i++) {
    printf("start: %lu, len: %lu, rstart: %lld, rlen: %d\n", block_start, block_len, readV[i].offset, readV[i].size);
    if (block_start > 0) {
      tlen = readV[i].offset + readV[i].size - block_start;
      if (tlen > READV_BUFFER_SIZE || block_len > READV_BUFFER_SIZE || readV[i].offset < block_start) {
        chunks_data_read = process_block(block_start, block_len, chunks_to_read, readV);
        if (chunks_data_read > 0) {
          data_read += chunks_data_read;
        } else {
          //log("Error while reading block at %ul, got %d\n", block_start, chunks_data_read);
          printf("Emergency exit, %ld bytes\n", chunks_data_read);
          return chunks_data_read;
        }
        block_start = -1;
        block_len = 0;
        tlen = readV[i].offset + readV[i].size - block_start;
        chunks_to_read.clear();
      }
      chunks_to_read.push_back(i);
      if (block_start < 0) {
        block_start = readV[i].offset;
      }
      if (tlen > block_len) {
        block_len = tlen;
      }
    } else {
      block_start = readV[i].offset;
      block_len = readV[i].size;
      chunks_to_read.push_back(i);
    }
  }
  if (block_len > 0) {
    chunks_data_read = process_block(block_start, block_len, chunks_to_read, readV);
    if (chunks_data_read > 0) {
      data_read += chunks_data_read;
    } else {
      //log("Error while reading block at %ul, got %d\n", block_start, chunks_data_read);
      printf("Emergency exit, %ld bytes\n", chunks_data_read);
      return chunks_data_read;
    }
  }
  printf("All read %ld bytes\n", data_read);

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
