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

#include "XrdCeph/XrdCephPosix.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"

#include "XrdCeph/XrdCephOssFile.hh"
#include "XrdCeph/XrdCephOss.hh"

extern XrdSysError XrdCephEroute;

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
  ssize_t retval;
  std::cerr << "Read data: " << offset << "," << blen << std::endl;
  if (m_cephOss->m_useDefaultPreadAlg) {
    retval = ceph_posix_pread(m_fd, buff, blen, offset);
  } else {
    retval = ceph_posix_nonstriper_pread(m_fd, buff, blen, offset);
    if (-ENOENT == retval || -ENOTSUP == retval) {
      //This might be a sparse file, so let's try striper read
      retval = ceph_posix_pread(m_fd, buff, blen, offset);
      if (retval >= 0) {
        XrdCephEroute.Say("WARNING! The file seem to be sparse, this is not expected");
      }
    }
  }
  return retval;
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

ssize_t XrdCephOssFile::ReadV(XrdOucIOVec *readV, int n) {
  ssize_t retval;
  std::cerr << "Readv data: ";
  for (int i = 0; i < n; i++) {
    std::cerr << readV[i].offset << "," << readV[i].size << ";";
  }
  std::cerr << std::endl;
  if (m_cephOss->m_useDefaultReadvAlg) {
    retval = ceph_striper_readv(m_fd, readV, n);
  } else {
    retval = ceph_nonstriper_readv(m_fd, readV, n);
    if (-ENOENT == retval || -ENOTSUP == retval) {
      //This might be a sparse file, so let's try striper read
      retval = ceph_striper_readv(m_fd, readV, n);
      if (retval >= 0) {
        XrdCephEroute.Say("WARNING! The file seem to be sparse, this is not expected");
      }
    }
  }
  return retval;
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
