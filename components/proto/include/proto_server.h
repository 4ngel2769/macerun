#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum
{
    PROTO_STATE_HANDSHAKE = 0,
    PROTO_STATE_STATUS = 1,
    PROTO_STATE_LOGIN = 2,
    PROTO_STATE_PLAY = 3,
} proto_state_t;

typedef struct
{
    proto_state_t state;
    bool close_requested;
    char username[17];
} proto_connection_t;

typedef struct
{
    uint16_t protocol_version;
    uint8_t max_players;
    uint8_t online_players;
    char motd[64];
} proto_server_info_t;

typedef bool (*proto_send_callback_t)(void *context,
                                      int socket_fd,
                                      const uint8_t *data,
                                      size_t length);

void proto_connection_reset(proto_connection_t *connection);

void proto_handle_packet(proto_connection_t *connection,
                         const uint8_t *packet,
                         size_t packet_length,
                         int socket_fd,
                         const proto_server_info_t *server,
                         proto_send_callback_t send_fn,
                         void *send_context);
