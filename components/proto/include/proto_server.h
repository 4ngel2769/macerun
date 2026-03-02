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
    bool joined_play;
    bool awaiting_keepalive;
    int32_t entity_id;
    int64_t uuid_most;
    int64_t uuid_least;
    int64_t last_keepalive_id;
    uint64_t next_keepalive_ms;
    uint64_t keepalive_deadline_ms;
    uint64_t last_activity_ms;
    double pos_x;
    double pos_y;
    double pos_z;
    float yaw;
    float pitch;
    bool on_ground;
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

typedef bool (*proto_broadcast_callback_t)(void *context,
                                           int source_socket_fd,
                                           const uint8_t *data,
                                           size_t length);

void proto_connection_reset(proto_connection_t *connection);

void proto_handle_packet(proto_connection_t *connection,
                         const uint8_t *packet,
                         size_t packet_length,
                         int socket_fd,
                         const proto_server_info_t *server,
                         proto_send_callback_t send_fn,
                         proto_broadcast_callback_t broadcast_fn,
                         void *send_context,
                         uint64_t now_ms);

void proto_tick_connection(proto_connection_t *connection,
                           int socket_fd,
                           const proto_server_info_t *server,
                           proto_send_callback_t send_fn,
                           void *send_context,
                           uint64_t now_ms);
