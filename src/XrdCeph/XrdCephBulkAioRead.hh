#include <string>
#include <map>
#include <list>
#include <tuple>
#include <rados/librados.hpp>

typedef std::tuple<ceph::bufferlist*, char*, int*> ReadOpData;
typedef void (*logfunc_pointer) (char *, ...);

class bulkAioRead {
  public:
  bulkAioRead(librados::IoCtx *ct, logfunc_pointer ptr, std::string filename, size_t block_size);
  ~bulkAioRead();

  void clear();
  void wait_for_complete();
  ssize_t get_results();
  int read(void *out_buf, size_t size, off64_t offset);

  private:
  int addRequest(std::string obj_name, char *out_buf, size_t size, off64_t offset);
  librados::IoCtx* context;
  std::list<ReadOpData> buffers;
  std::map<std::string, std::pair<librados::ObjectReadOperation*, librados::AioCompletion*>> operations;
  std::string file_name;
  logfunc_pointer log_func; 
  size_t block_size;
};
