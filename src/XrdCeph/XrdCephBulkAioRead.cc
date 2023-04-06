#include "XrdCephBulkAioRead.hh"


bulkAioRead::bulkAioRead(librados::IoCtx *ct, logfunc_pointer logwrapper, std::string filename, size_t object_sz) {
  /**
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
}

bulkAioRead::~bulkAioRead() {
  /**
   * Destructor. Just clears dynamically allocated memroy.
   */
  clear();
}

void bulkAioRead::clear() {
  /**
   * Clear all dynamically alocated memory
   */
  buffers.clear();
  operations.clear();
}

int bulkAioRead::addRequest(size_t obj_idx, char* out_buf, size_t size, off64_t offset) {
  /**
   * Prepare read request for a single ceph object. Private method.
   *
   * Method will allocate all (well, almost, except the string for the object name)
   * necessary objects to submit read request to ceph. To submit the requests use
   * `submit_and_wait_for_complete` method.
   *
   * @param obj_idx  number of the object (starting from zero) to read
   * @param out_buf  output buffer, where read results should be stored
   * @param size     number of bytes to read
   * @param offset   offset in bytes where the read should start. Note that the offset is local to the
   *                 ceph object. I.e. if offset is 0 and object number is 1, yo'll be reading from the
   *                 start of the second object, not from the begining of the file.
   *
   * @return         zero on success, negative error code on failure
   */

  try{
    auto &op_data = operations[obj_idx];
    //When we start using C++17, the next two lines can be merged
    buffers.emplace_back(out_buf);
    auto &buf = buffers.back();
    op_data.ceph_read_op.read(offset, size, &buf.bl, &buf.rc);
  } catch (std::bad_alloc&) {
   log_func((char*)"Memory allocation failed while reading file %s", file_name.c_str());
   return -ENOMEM;
  }
  return 0;
}

int bulkAioRead::submit_and_wait_for_complete() {
  /**
   * Submit previously prepared read requests and wait for their completion
   *
   * To prepare read requests use `read` or `addRequest` methods.
   *
   * @return  zero on success, negative error code on failure
   *
   */

  std::string obj_name;

  for (auto &op_data: operations) {
    size_t obj_idx = op_data.first;
    //16 bytes for object hex number, 1 for dot and 1 for null-terminator
    char object_suffix[18];
    int sp_bytes_written;
    sp_bytes_written = snprintf(object_suffix, sizeof(object_suffix), ".%016zx", obj_idx);
    if (sp_bytes_written >= (int) sizeof(object_suffix)) {
      log_func((char*)"Can not fit object suffix into buffer for file %s -- too big\n", file_name.c_str());
      return -EFBIG;
    } 

    try {
      obj_name =  file_name + std::string(object_suffix);
    } catch (std::bad_alloc&) {
      log_func((char*)"Can not create object string for file %s)", file_name.c_str());
      return -ENOMEM;
    }
    context->aio_operate(obj_name, op_data.second.cmpl, &op_data.second.ceph_read_op, 0);
  }

  for (auto &op_data: operations) {
    op_data.second.cmpl.wait_for_complete();
    int rval = op_data.second.cmpl.get_return_value();
    if (rval < 0) {
      log_func((char*)"Read of the object %ld for file %s failed", op_data.first, file_name.c_str());
      return rval;
    }
  }
  return 0;
}

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

  ssize_t res = 0;
  for (ReadOpData &op_data: buffers) {
    if (op_data.rc < 0) {
      //Is it possible to get here?
      log_func((char*)"One of the reads failed with rc %d", op_data.rc);
      return op_data.rc;
    }
    op_data.bl.begin().copy(op_data.bl.length(), op_data.out_buf);
    res += op_data.bl.length();
  }
  return res;
}

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
    chunk_len = std::min(req_len, object_size - chunk_start);

    int rc;
    rc = addRequest(start_block, buf_ptr, chunk_len, chunk_start);
    if (rc < 0) {
      log_func((char*)"Unable to submit async read request, rc=%d\n", rc);
      return rc;
    }
    buf_pos += chunk_len;
    if (buf_pos > req_size) {
      log_func((char*)"Internal bug! Attempt to read %lu data for block (%lu, %lu) of file %s\n", buf_pos, offset, req_size, file_name.c_str());
      return -EINVAL;
    }
    buf_ptr += chunk_len;

    start_block++;
    chunk_start = 0;
    req_len = req_len - chunk_len;
  }
  return 0;   
}
