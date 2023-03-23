#include "XrdCephBulkAioRead.hh"

bulkAioRead::bulkAioRead(librados::IoCtx *ct, logfunc_pointer logwrapper, std::string filename, size_t object_sz) {
  context = ct;
  object_size = object_sz;
  file_name = filename;
  log_func = logwrapper;
};

bulkAioRead::~bulkAioRead() {
  clear();
};

void bulkAioRead::clear() {
  for (ReadOpData i: buffers){
    delete std::get<0>(i);
    delete std::get<2>(i);
  }
  buffers.clear();
  for (auto const& tup: operations) {
    librados::ObjectReadOperation *op = std::get<0>(tup.second);
    librados::AioCompletion *cmpl = std::get<1>(tup.second);
    cmpl->release();
    delete op;
  }
  operations.clear();
};

int bulkAioRead::addRequest(std::string objname, char* out_buf, size_t size, off64_t offset) {
  ceph::bufferlist *bl;
  int *retval;
  librados::ObjectReadOperation *rop;

  try {
    bl = new ceph::bufferlist();
  } catch (std::bad_alloc&) {
    log_func((char*)"Can not allocate buffer for read (%lu, %lu)", offset, size);
    return -ENOMEM;
  }

  try {
    retval = new int;
  } catch (std::bad_alloc&) {
    log_func((char*)"Can not allocate int for read's retval (%lu, %lu)", offset, size);
    delete bl;
    return -ENOMEM;
  }

  auto tup = operations.find(objname);
  if (operations.end() == tup) {
    try {
      rop = new librados::ObjectReadOperation;
    } catch (std::bad_alloc&) {
      log_func((char*)"Can not allocate int for read object op (%lu, %lu)", offset, size);
      delete bl;
      delete retval;
      return -ENOMEM;
    }

    librados::AioCompletion *cmpl = librados::Rados::aio_create_completion();
    if (0 == cmpl) {
      log_func((char*)"Can not create completion for read (%lu, %lu)", offset, size);
      delete bl;
      delete retval;
      delete rop;
      return -ENOMEM;
    }
    operations.insert( {objname, std::make_pair(rop, cmpl)} );
  } else {
    rop = std::get<0>(tup->second);
  }
  buffers.push_back( std::make_tuple(bl, out_buf, retval) );

  rop->read(offset, size, bl, retval);
  return 0;
};

void bulkAioRead::wait_for_complete() {
  std::string obj_name;
  librados::AioCompletion* cmpl;
  librados::ObjectReadOperation* op;
  for (auto const& tup: operations) {
    obj_name = tup.first;
    op = std::get<0>(tup.second);
    cmpl = std::get<1>(tup.second);
    context->aio_operate(obj_name, cmpl, op, 0);
  }

  for (auto const& tup: operations) {
    cmpl = std::get<1>(tup.second);
    cmpl->wait_for_complete();
  }
};

ssize_t bulkAioRead::get_results() {
  ceph::bufferlist* bl;
  char *buf;
  int *rc;
  ssize_t res = 0;
  for (ReadOpData i: buffers) {
    bl = std::get<0>(i);
    buf = std::get<1>(i);
    rc = std::get<2>(i);
    if (*rc < 0) {
      log_func((char*)"One of the reads failed with rc %d", rc);
      return *rc;
    }
    bl->begin().copy(bl->length(), buf);
    res += bl->length();
  }
  return res;
};

int bulkAioRead::read(void* out_buf, size_t req_size, off64_t offset) {

  size_t start_block, last_block, buf_pos, chunk_len, chunk_start, req_len;
  char *buf_ptr;

  if (req_size == 0) {
    return -EINVAL;
  }

  req_len = req_size;
  start_block = offset / object_size;
  last_block = (offset + req_len - 1) / object_size;
  buf_ptr = (char*) out_buf;
  buf_pos = 0;
  chunk_start = offset % object_size;

  while (start_block <= last_block) {
    //16 bytes for object hex number, 1 for dot and 1 for null-terminator
    char object_suffix[18];
    int sp_bytes_written;
    sp_bytes_written = snprintf(object_suffix, sizeof(object_suffix), ".%016zx", start_block);
    if (sp_bytes_written > (int) sizeof(object_suffix)) {
      log_func((char*)"Can not fit object suffix into buffer for file %s\n", file_name.c_str());
      return -EFBIG;
    } 

    std::string obj_name;
    obj_name =  file_name + std::string(object_suffix);

    chunk_len = std::min(req_len, object_size - chunk_start);

    int rc;
    rc = addRequest(obj_name, buf_ptr, chunk_len, chunk_start);
    if (rc < 0) {
      log_func((char*)"Unable to submit async read request, rc=%d\n", rc);
      return rc;
    }
    buf_pos += chunk_len;
    if (buf_pos > req_size) {
      log_func((char*)"Internal bug! Attempt to read %lu data for block (%lu, %lu) of object %s\n", buf_pos, offset, req_size, obj_name.c_str());
      return -EINVAL;
    }
    buf_ptr += chunk_len;

    start_block++;
    chunk_start = 0;
    req_len = req_len - chunk_len;
  }
  return 0;   
};
