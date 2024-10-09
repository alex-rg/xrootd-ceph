#include <string>
#include <rados/librados.hpp>

#include "XrdCeph/XrdCephPosix.hh"

#define MAX_HOST_NAME 128

/**
 * Lock or unlock a file using rados.
 * Host name is used as lock cookie.
 * A separate object named <filename>.XrdCeph_Exclusive_lock is for locking (to prevent issues with striper)
 * Can be useful to prevent simultaneous writes/deletes of the same file.
 *
 */



struct XrdCephFileLock {
  const std::string lock_ext = std::string(".XrdCeph_Exlcusive_lock");
  //const std::string lock_ext = std::string(".0000000000000000");
  const std::string XrdCeph_object_lock = "xrdceph.lock";
  CephFile fr;
  std::string obj_name;
  std::string cookie;
  librados::IoCtx* ioctx;

  XrdCephFileLock(const CephFile file,  librados::IoCtx*);
  int acquire();
  int release();
};
