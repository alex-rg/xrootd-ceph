#include <string>
#include <map>
#include <list>
#include <tuple>
#include <rados/librados.hpp>


class bulkAioRead {
  //typedef std::tuple<ceph::bufferlist*, char*, int*> ReadOpData;
  typedef void (*logfunc_pointer) (char *, ...);

  /**
   * Class is used to execute read operations against rados striper files *without* usage of rados striper.
   * Reads are based on ceph read operations.
   *
   * The interface is similar to the one that ceph's read operation objects has:
   * 1. Instantiate the object.
   * 2. Declare read operations using 'read' method, providing the output buffers, offset and length.
   * 3. Submitn operation and wait for results using 'submit_and_wait_for_complete' method.
   * 4. Copy results to buffers with 'get_results' method. 
   *
   * WARNING: there is no copy/move constructor in the class, so do not use temporary objects for initialization
   * (i.e. something like `bulkAioRead rop = bulkAioRead(...);` will not work, use `bulkAioRead rop(...);` instead).
   */ 
  public:
  bulkAioRead(librados::IoCtx *ct, logfunc_pointer ptr, std::string filename, size_t object_size);
  ~bulkAioRead();

  void clear();
  int submit_and_wait_for_complete();
  ssize_t get_results();
  int read(void *out_buf, size_t size, off64_t offset);

  private:
  //Completion pointer
  class CmplPtr {
    librados::AioCompletion *ptr;
    bool loaded = false;
    public:
    CmplPtr() {
      ptr = librados::Rados::aio_create_completion();
      if (NULL == ptr) {
        throw std::bad_alloc();
      }
    }
    ~CmplPtr() {
      if (loaded && ! ptr->is_complete()) {
        this->wait_for_complete();
      }
      ptr->release();
    }
    void wait_for_complete() {
      ptr->wait_for_complete();
    }
    int get_return_value() {
      return ptr->get_return_value();
    }
    operator librados::AioCompletion*() {
      //If the object was converted to AioCompletion, we suppose it was passed to
      //the read operation, and therefore set the flag.
      loaded = true;
      return ptr;
    }
  };

  //Ceph read operation + completion
  struct CephOpData {
    librados::ObjectReadOperation ceph_read_op;
    CmplPtr cmpl;
  };

  //Data for an individual read -- ceph's buffer, client's buffer and return code
  struct ReadOpData {
    ceph::bufferlist bl;
    char* out_buf;
    int rc;
    ReadOpData(char* output_buf): out_buf(output_buf) {};
  };

  

  int addRequest(size_t obj_idx, char *out_buf, size_t size, off64_t offset);
  librados::IoCtx* context;
  std::list<ReadOpData> buffers;

  std::map<size_t, CephOpData> operations;

  std::string file_name;
  logfunc_pointer log_func; 
  size_t object_size;
};
