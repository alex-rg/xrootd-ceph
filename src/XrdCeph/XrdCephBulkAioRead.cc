#include "XrdCephBulkAioRead.hh"

bulkAioRead::bulkAioRead(librados::IoCtx *ct, logfunc_pointer logwrapper, std::string filename, size_t object_sz) {
  /*
   * Constructor.
   *
   * @param ct                Rados IoContext object
   * @param logfunc_pointer   Pointer to the function that will be used for logging
   * @param filename          Name of the file (not ceph object!) that is supposed to be read
   * @param object_sz         Maximum size of the ceph objects that the file is split into
   *
   */
  context = ct;
  object_size = object_sz;
  file_name = filename;
  log_func = logwrapper;
};

bulkAioRead::~bulkAioRead() {
  /*
   * Destructor. Just clears dynamically allocate memroy.
   */
  clear();
};

void bulkAioRead::clear() {
  /**
   * Clear all dynamically alocated memory
   */
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
  /**
   * Prepare read request for a single ceph object. Private method.
   *
   * Method will allocate all necessary objects to submit read request to ceph.
   * To submit the requests use `submit_and_wait_for_complete` method.
   *
   * @param objname  name of the object to read
   * @param out_buf  output buffer, where read results should be stored
   * @param size     number of bytes to read
   * @param offset   offset in bytes where the read should start. Note that the offset is local to the
   *                 ceph object. I.e. if offset is 0 and object is `file1.0000000000000001`, yo'll be
   *                 reading from the start of the second object of file1, not from its begining.
   *
   * @return         zero on success, negative error code on failure
   */
  ceph::bufferlist *bl;
  int *retval = NULL;
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

  auto op_data = operations.find(objname);
  if (operations.end() == op_data) {
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
    rop = std::get<0>(op_data->second);
  }
  buffers.push_back( std::make_tuple(bl, out_buf, retval) );

  rop->read(offset, size, bl, retval);
  return 0;
};

void bulkAioRead::submit_and_wait_for_complete() {
  /**
   * Submit previously prepared read requests and wait for their completion
   *
   * To prepare read requests use `read` or `addRequest` methods.
   *
   */

  std::string obj_name;
  librados::AioCompletion* cmpl;
  librados::ObjectReadOperation* op;
  for (auto const& op_data: operations) {
    obj_name = op_data.first;
    op = std::get<0>(op_data.second);
    cmpl = std::get<1>(op_data.second);
    context->aio_operate(obj_name, cmpl, op, 0);
  }

  for (auto const& op_data: operations) {
    cmpl = std::get<1>(op_data.second);
    cmpl->wait_for_complete();
  }
};

ssize_t bulkAioRead::get_results() {
  /**
   * Copy the results of executed read requests from ceph's bufferlists to client's buffers
   *
   * Note that this method should be called only after the submission and completion of read
   * requests, i.e. after `submit_and_wait_for_complete` method.
   *
   * @return  cumulative number of bytes read (by all read operations) on success, negative
   *          error code on failure
   *
   */

  ceph::bufferlist* bl;
  char *buf;
  int *rc;
  ssize_t res = 0;
  for (ReadOpData i: buffers) {
    bl = std::get<0>(i);
    buf = std::get<1>(i);
    rc = std::get<2>(i);
    if (*rc < 0) {
      //Is it possible to get here?
      log_func((char*)"One of the reads failed with rc %d", rc);
      return *rc;
    }
    bl->begin().copy(bl->length(), buf);
    res += bl->length();
  }
  return res;
};

int bulkAioRead::read(void* out_buf, size_t req_size, off64_t offset) {
  /**
   * Declare a read operation for file.
   *
   * Read coordinates are global, i.e. valid offsets are from 0 to the <file_size> -1, valid request sizes
   * are from 1 to <file_size> - <offset>. Method can be called multiple times to declare multiple read
   * operations on the same file.
   *
   * @param out_buf    output buffer, where read results should be stored
   * @param req_size   number of bytes to read
   * @param offset     offset in bytes where the read should start. Note that the offset is global,
   *                   i.e. refers to the whole file, not individual ceph objects
   *
   * @return  zero on success, negative error code on failure
   *
   */

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
