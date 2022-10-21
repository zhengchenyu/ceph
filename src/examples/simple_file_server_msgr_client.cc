#include <iostream>
#include <map>
#include <atomic>
#include <fstream>

#include "auth/DummyAuth.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/ceph_mutex.h"
#include "global/global_init.h"
#include "msg/Connection.h"
#include "msg/Dispatcher.h"
#include "msg/Message.h"
#include "msg/Messenger.h"
#include "msg/msg_types.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

const int g_nonce = 12345;

using std::string;

class TestClientDispatcher : public Dispatcher {
public:
  struct Session : public RefCountedObject {
    std::atomic<uint64_t> count;
    ConnectionRef con;

    explicit Session(ConnectionRef c): RefCountedObject(g_ceph_context), count(0), con(c) {
    }
    uint64_t get_count() { return count; }
  };

  ceph::mutex lock = ceph::make_mutex("TestClientDispatcher::lock");
  CephContext *cct;

  explicit TestClientDispatcher(): Dispatcher(g_ceph_context) {
  }
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
    switch (m->get_type()) {
      case CEPH_MSG_PING:
        std::cout << "simple_file_server_msgr_client::" << __func__ << "Received ping message" << std::endl;
        return true;
      default:
        return false;
    }
  }

  void ms_handle_fast_accept(Connection *con) override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
  }

  bool ms_dispatch(Message *m) override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
    MCommandReply *reply = dynamic_cast<MCommandReply*>(m);
    if (reply) {
      std::cout << "simple_file_server_msgr_client::" << __func__ << " replay from command is (" << reply->r << ", " << reply->rs << ")" << std::endl;
    } else {
      std::cout << "simple_file_server_msgr_client::" << __func__ << " message is " << m << std::endl;
    }
    return true;
  }
  bool ms_handle_reset(Connection *con) override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
    return true;
  }
  void ms_handle_remote_reset(Connection *con) override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
  }
  bool ms_handle_refused(Connection *con) override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
    return false;
  }
  void ms_fast_dispatch(Message *m) override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
    MCommandReply *reply = dynamic_cast<MCommandReply*>(m);
    if (reply) {
      std::cout << "simple_file_server_msgr_client::" << __func__ << " replay from command is (" << reply->r << ", " << reply->rs << ")" << std::endl;
    } else {
      std::cout << "simple_file_server_msgr_client::" << __func__ << " message is " << m << std::endl;
    }
  }

  int ms_handle_authentication(Connection *con) override {
    std::cout << "simple_file_server_msgr_client::" << __func__ << std::endl;
    return 1;
  }
};

int main(int argc, const char **argv) {auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->ms_async_op_threads = 1;
  g_ceph_context->_conf->ms_connection_ready_timeout = 100000;       // Ignore timeout when debug
  g_ceph_context->_conf->ms_connection_idle_timeout = 100000;
  g_ceph_context->_conf.apply_changes(nullptr);

  Messenger *client_msgr = Messenger::create(g_ceph_context, "async+posix", entity_name_t::CLIENT(0), "client", g_nonce);
  client_msgr->set_default_policy(Messenger::Policy::lossless_client(0));
  DummyAuthClientServer dummy_auth(g_ceph_context);
  dummy_auth.auth_registry.refresh_config();
  client_msgr->set_auth_client(&dummy_auth);

  TestClientDispatcher cli_dispatcher;
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:33333");
  bind_addr.set_nonce(12345);
  ConnectionRef c2s = client_msgr->connect_to(CEPH_ENTITY_TYPE_OSD,entity_addrvec_t{bind_addr});
  sleep(10);
  MCommand *fom = new MCommand();
  fom->cmd = {"/root/ceph/simple-file-server.txt", "I am writting by simple file server!"};
  for (int i = 0; i < 1; i++) {
    c2s->send_message(fom);
    sleep(1);
  }
  client_msgr->wait();

  return 0;
}