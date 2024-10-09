#include "XrdCeph/XrdCephFileLock.hh"

XrdCephFileLock::XrdCephFileLock(const CephFile file, librados::IoCtx* ctx) {
  /**
   * Constructor.
   *
   * @param file    ceph file description
   *
   */

    fr = file;
    obj_name = fr.name + lock_ext;

    char host[128];
    gethostname(host, MAX_HOST_NAME);
    cookie = std::string(host);
    ioctx = ctx; 
}

int XrdCephFileLock::acquire() {
  /**
   * Acquire the lock.
   *
   */

    struct timeval lock_lifetime;
    lock_lifetime.tv_sec = 3600*5;
    lock_lifetime.tv_usec = 0;

    int rc = ioctx->lock_exclusive(obj_name, XrdCeph_object_lock, cookie, "", &lock_lifetime, 0);
    return rc;
}

int XrdCephFileLock::release() {
  /**
   * Release the lock.
   *
   */
    int rc = ioctx->unlock(obj_name, XrdCeph_object_lock, cookie);
    if (0 == rc) {
       rc = ioctx->remove(obj_name);
    }
    return rc;
}
