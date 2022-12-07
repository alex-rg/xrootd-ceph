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

#include <list>
#include <map>

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


struct DataChunk
    {
    int start;
    int len;
    int orig_block;
    DataChunk(int st, int ln, int ob)
        {
        start = st;
        len = ln;
        orig_block = ob;
        };
    };

struct ReadVBuffer
    {
    char *ptr;
    ReadVBuffer(int size)
        {
        ptr = new char[size];
        }

    ~ReadVBuffer()
        {
        delete[] ptr;
        }
    };

struct DataPointers
    {
    char** pointers;
    DataPointers(int size, XrdOucIOVec *readV)
        {
        pointers = new char*[size];
        for (int i=0; i<size; i++)
            {
            pointers[i] = readV[i].data;
            }
        }
    ~DataPointers()
        {
        delete[] pointers;
        }

    char*& operator[] (int idx)
        {
        return pointers[idx];
        }
    };


typedef std::map<  int, std::list< DataChunk >  > BlockDict;
typedef std::list< DataChunk > VreadChunks;


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

ssize_t XrdCephOssFile::ReadV(XrdOucIOVec *readV, int n)
{
    /*
    To allow effective readv from ceph, we are going to read data
    from storage in blocks of the given size, and then extract chunks
    requested in readv from these blocks.
    */
    //FIXME: use value from config
    const unsigned int block_size = 16*1024*1024;
    unsigned int idx = 0;
    BlockDict blocks;
    /*
    Data from ceph is going to be read in blocks of size block_size.
    Here we analize what data is to be read. The results are stored
    in BlockDict map in the following form:
    <block_number> : <DataChunk>
    Where <DataChunk> is the structure containing information of the
    start start of useful data in the block, its length and original
    block number (i.e. number in the readv request).
    */
    for (int i = 0; i < n; i++)
        {
        ssize_t start_block, end_block;
        off_t offset = readV[i].offset;
        ssize_t len = readV[i].size;
        start_block = offset / block_size;
        end_block = (offset + len - 1) / block_size;
        while (start_block <= end_block)
            {
            ssize_t chunk_start, chunk_len;
            chunk_start = offset % block_size;
            if (len < block_size - chunk_start)
                chunk_len = len;
            else
                chunk_len = block_size - chunk_start;
            try
                {
                blocks.at(start_block).push_back(DataChunk(chunk_start, chunk_len, idx));
                }
            catch (std::out_of_range&)
                {
                blocks[start_block] =  { DataChunk(chunk_start, chunk_len, idx) };
                }
            start_block++;
            offset = start_block*block_size;
            len = len - chunk_len;
            }
        idx++;
        }

    DataPointers pointers = DataPointers(n, readV);
    ReadVBuffer buf = ReadVBuffer(block_size);
    char * buf_ptr;
    ssize_t requested_data_read = 0;

    /*
    Read the blocks and place their chunks into corresponding buffers.
    */
    for (auto i = blocks.begin(); i != blocks.end(); i++)
        {
        ssize_t data_read = Read((void *)buf.ptr, (off_t) block_size*(i->first), (size_t)block_size);
        //FIXME: we should not request data past end of file.
        if (data_read < 0)
            {
            return data_read;
            }
        for (auto j = i->second.begin(); j != i->second.end(); j++)
            {
            if (j->start + j->len > data_read)
                {
                return -ESPIPE;
                }
            buf_ptr = buf.ptr;
            buf_ptr += j->start;
            memcpy(pointers[j->orig_block], buf_ptr, j->len);
            pointers[j->orig_block] += j->len;
            requested_data_read += j->len;
            }
        }

    return requested_data_read;
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
