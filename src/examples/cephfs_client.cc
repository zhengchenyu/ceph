#include <iostream>
#include <memory>
#include <string>
#include "include/cephfs/libcephfs.h"

int main(int argc, const char **argv) {
  struct ceph_mount_info *cmount;
  int r = ceph_create(&cmount, nullptr);
  if (r < 0) {
    std::cerr << "create ceph mount info failed." << std::endl;
    return r;
  }
  r = ceph_conf_read_file(cmount, nullptr);
  if (r < 0) {
    std::cerr << "ceph_conf_read_file Failed." << std::endl;
    return EXIT_FAILURE;
  }
  r = ceph_conf_parse_env(cmount, nullptr);
  if (r < 0) {
    std::cerr << "ceph_conf_parse_env Failed." << std::endl;
    return EXIT_FAILURE;
  }
  r = ceph_mount(cmount, "/");
  if (r < 0) {
    std::cerr << "ceph_mount Failed." << std::endl;
    return EXIT_FAILURE;
  }
  r = ceph_chmod(cmount, "/", 01777);
  if (r < 0) {
    std::cerr << "chmod / Failed." << std::endl;
    return EXIT_FAILURE;
  }

  r = ceph_mkdirs(cmount, "/cephfs_client", 0777);
  if (r < 0) {
    std::cerr << "chmod /cephfs_client Failed." << std::endl;
    return EXIT_FAILURE;
  }

  ceph_mkdirs(cmount, "/cephfs_client/dir1", 0777);
  if (r < 0) {
    std::cerr << "chmod /cephfs_client/dir1 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  ceph_mkdirs(cmount, "/cephfs_client/dir2", 0777);
  if (r < 0) {
    std::cerr << "chmod /cephfs_client/dir2 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  int fd = ceph_open(cmount, "/cephfs_client/dir1/file1", O_WRONLY|O_CREAT, 0666);
  if (fd <= 0) {
    std::cerr << "open /cephfs_client/dir1/file1 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  std::string context = "hello, I am cephfs client!";
  r = ceph_write(cmount, fd, context.c_str(), context.size(), 0);
  if (r <= 0) {
    std::cerr << "write /cephfs_client/dir1/file1 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  r = ceph_close(cmount, fd);
  if (r < 0) {
    std::cerr << "close /cephfs_client/dir1/file1 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  fd = ceph_open(cmount, "/cephfs_client/dir1/file1", O_RDONLY, 0);
  if (fd <= 0) {
    std::cerr << "open /cephfs_client/dir1/file1 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  char in_buf[128];
  r = ceph_read(cmount, fd, in_buf, sizeof(in_buf), 0);
  if (r < 0) {
    std::cerr << "read /cephfs_client/dir1/file1 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  std::cout << "Read content from /cephfs_client/dir1/file1: " << in_buf << std::endl;

  r = ceph_close(cmount, fd);
  if (r < 0) {
    std::cerr << "close /cephfs_client/dir1/file1 Failed." << std::endl;
    return EXIT_FAILURE;
  }

  return r;
}