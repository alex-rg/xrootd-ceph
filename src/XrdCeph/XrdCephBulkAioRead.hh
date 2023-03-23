#include <string>
#include <map>
#include <list>
#include <tuple>
#include <rados/librados.hpp>


class bulkAioRead {
  typedef std::tuple<ceph::bufferlist*, char*, int*> ReadOpData;
  typedef void (*logfunc_pointer) (char *, ...);

  /*
 * Class is used to execute read operations against rados striper files *without* usage of rados striper.
 * Reads are based on ceph read operations.
 *
 * The interface is similar to the one that ceph's read operation objects has:
 * 1. Instantiate the object.
 * 2. Declare read operations using 'read' method, providing the output buffers, offset and length.
 * 3. Submitn operation and wait for results using 'submit_and_wait_for_complete' method.
 * 4. Copy results to buffers with 'get_results' method. 
  */ 
  public:
  bulkAioRead(librados::IoCtx *ct, logfunc_pointer ptr, std::string filename, size_t object_size);
  ~bulkAioRead();

  void clear();
  int submit_and_wait_for_complete();
  ssize_t get_results();
  int read(void *out_buf, size_t size, off64_t offset);

  private:
  int addRequest(size_t obj_idx, char *out_buf, size_t size, off64_t offset);
  librados::IoCtx* context;
  std::list<ReadOpData> buffers;
  /* [ 
 *     (<ceph_bufferlist>, <client_buffer>, <retval_pointer>),
 *     ...
 *   ]
 * */

  std::map<size_t, std::pair<librados::ObjectReadOperation*, librados::AioCompletion*>> operations;
  /* {
 *     <object_idx>: (<read_op>, <completion>)
 *     ...
 *   } 
 * */

  std::string file_name;
  logfunc_pointer log_func; 
  size_t object_size;
};
