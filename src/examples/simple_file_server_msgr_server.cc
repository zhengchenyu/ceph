#include <iostream>
#include <map>
#include <atomic>
#include <fstream>
#include <string_view>
#include <vector>

#include "auth/DummyAuth.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/ceph_mutex.h"
#include "global/global_init.h"
#include "msg/Message.h"
#include "msg/Connection.h"
#include "msg/Dispatcher.h"
#include "msg/Messenger.h"
#include "msg/msg_types.h"
#include "messages/MCommand.h"
#include "messages/MCommandReply.h"

const int g_nonce = 12345;

class TestServerDispatcher : public Dispatcher {
public:
  struct Session : public RefCountedObject {
    std::atomic<uint64_t> count;
    ConnectionRef con;

    explicit Session(ConnectionRef c): RefCountedObject(g_ceph_context), count(0), con(c) {
    }
    uint64_t get_count() { return count; }
  };

  ceph::mutex lock = ceph::make_mutex("TestServerDispatcher::lock");
  CephContext *cct;

  explicit TestServerDispatcher(): Dispatcher(g_ceph_context) {
  }
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    switch (m->get_type()) {
      case CEPH_MSG_PING:
        //cout << "Received ping message, " << m->get_source() << endl;
        std::cout << "simple_file_server_msgr_server::" << __func__ << "Received ping message" << std::endl;
        return true;
      default:
        return false;
    }
  }

  void ms_handle_fast_accept(Connection *con) override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    if (!con->get_priv()) {
      con->set_priv(RefCountedPtr{new Session(con), false});
    }
  }

  bool ms_dispatch(Message *m) override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    auto priv = m->get_connection()->get_priv();
    auto s = static_cast<Session*>(priv.get());
    if (!s) {
      s = new Session(m->get_connection());
      priv.reset(s, false);
      m->get_connection()->set_priv(priv);
    }
    s->count++;
    std::cout << "simple_file_server_msgr_server::" << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    reply_message(m);

    std::lock_guard l{lock};
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    return true;
  }
  void ms_handle_remote_reset(Connection *con) override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
  }
  bool ms_handle_refused(Connection *con) override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    return false;
  }
  void ms_fast_dispatch(Message *m) override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    auto priv = m->get_connection()->get_priv();
    auto s = static_cast<Session*>(priv.get());
    if (!s) {
      s = new Session(m->get_connection());
      priv.reset(s, false);
      m->get_connection()->set_priv(priv);
    }
    s->count++;
    std::cout << "simple_file_server_msgr_server::" << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    reply_message(m);

    m->put();
    std::lock_guard l{lock};
  }

  int ms_handle_authentication(Connection *con) override {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    return 1;
  }

  void reply_message(Message *m) {
    std::cout << "simple_file_server_msgr_server::" << __func__ << std::endl;
    MCommand *cmd = dynamic_cast<MCommand*>(m);
    std::vector<std::string> cmds = cmd->cmd;
    if (cmd == nullptr) {
      MCommandReply *reply = new MCommandReply(1, "File not found!");
      m->get_connection()->send_message(reply);
    } else if (cmds.size() != 2) {
      MCommandReply *reply = new MCommandReply(1, "Parameter not right!");
      m->get_connection()->send_message(reply);
    } else {
      std::ofstream of;
      of.open(cmds[0]);
      of << cmds[1];
      of.close();
      MCommandReply *reply = new MCommandReply(0, "Write file " + cmds[0] + " success!");
      m->get_connection()->send_message(reply);
    }
  }
};

int main(int argc, const char **argv) {
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->ms_async_op_threads = 1;
  g_ceph_context->_conf->ms_connection_ready_timeout = 100000;       // Ignore timeout when debug
  g_ceph_context->_conf->ms_connection_idle_timeout = 100000;
  g_ceph_context->_conf.apply_changes(nullptr);

  Messenger *server_msgr = Messenger::create(g_ceph_context, "async+posix", entity_name_t::OSD(0), "server", g_nonce);
  server_msgr->set_default_policy(Messenger::Policy::stateless_server(0));
  DummyAuthClientServer dummy_auth(g_ceph_context);
  dummy_auth.auth_registry.refresh_config();
  server_msgr->set_auth_server(&dummy_auth);
  // server_msgr->set_require_authorizer(false);

  TestServerDispatcher srv_dispatcher;
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:33333");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  std::cout << "simple file server started!" << std::endl;
  std::cout << "server_msgr: mytype is " << server_msgr->get_mytype() << ", myaddrs is " << server_msgr->get_myaddrs() << std::endl;
  server_msgr->wait();
}