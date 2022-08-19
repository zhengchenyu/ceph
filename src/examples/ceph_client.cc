#include <iostream>

#include <rados/librados.hpp>

int main(int argc, const char **argv) {
  int ret = 0;

  librados::Rados cluster;
  const char cluster_name[] = "ceph";
  const char user_name[] = "client.admin";
  const char pool_name[] = "default";
  const char key[] = "key4test";
  uint64_t flags = 0;

  // 1 Init cluster
  ret = cluster.init2(user_name, cluster_name, flags);
  if (ret < 0) {
    std::cerr << "Initial cluster Failed." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Initial cluster success." << std::endl;
  }

  // 2 Read ceph configuration
  ret = cluster.conf_read_file("/etc/ceph/ceph.conf");
  if (ret < 0) {
    std::cerr << "Read conf file failed." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Read conf file success." << std::endl;
  }

  // 3. Read command line
  ret = cluster.conf_parse_argv(argc, argv);
  if (ret < 0) {
    std::cerr << "Couldn't parse command line." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Parse command line success." << std::endl;
  }

  // 4. Connect cluster
  ret = cluster.connect();
  if (ret < 0) {
    std::cerr << "Connect to the cluster failed." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Connect to the cluster success." << std::endl;
  }

  // 5. Create IO context
  librados::IoCtx io_ctx;
  ret = cluster.ioctx_create(pool_name, io_ctx);
  if (ret < 0) {
    std::cerr << "Create IO context failed." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Create IO context success." << std::endl;
  }

  // 6. Write
  librados::bufferlist wbuffer;
  wbuffer.append("Hello!");
  const char * str;
  ret = io_ctx.write_full(key, wbuffer);
  if (ret < 0) {
    std::cerr << "Write failed." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Write " << key <<" success" << std::endl;
  }

  // 7. Read
  librados::bufferlist rbuffer;
  int len = 128;
  librados::AioCompletion *read_completion = librados::Rados::aio_create_completion();
  ret = io_ctx.aio_read(key, read_completion, & rbuffer, len, 0);
  if (ret < 0) {
    std::cerr << "Start read failed." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Start read success." << std::endl;
  }
  read_completion->wait_for_complete();
  ret = read_completion->get_return_value();
  if (ret < 0) {
    std::cerr << "Read failed." << std::endl;
    return EXIT_FAILURE;
  } else {
    std::cout << "Read " << key <<" succuess, value is " << rbuffer.c_str() << std::endl;
  }

}
