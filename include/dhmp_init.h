#ifndef DHMP_INIT_H
#define DHMP_INIT_H
#include "dhmp_server.h"

struct dhmp_server * dhmp_server_init(size_t server_id);
struct dhmp_client * dhmp_client_init(size_t size,  bool is_mica_cli);
#endif