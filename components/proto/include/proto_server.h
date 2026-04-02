#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "server_limits.h"

typedef struct
{
    int32_t x;
    int32_t z;
} proto_chunk_coord_t;

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
    double prev_pos_x;
    double prev_pos_y;
    double prev_pos_z;
    bool pending_swing_animation;
    bool pending_attack_event;
    int32_t pending_attack_target_entity_id;
    uint64_t next_attack_allowed_ms;
    float yaw;
    float pitch;
    float health;
    int32_t food_level;
    float food_saturation;
    uint64_t next_hunger_decay_ms;
    uint64_t next_health_regen_ms;
    uint64_t next_starvation_damage_ms;
    uint64_t next_void_damage_ms;
    bool on_ground;
    bool chunk_stream_initialized;
    int32_t chunk_center_x;
    int32_t chunk_center_z;
    bool has_previous_chunk_center;
    int32_t previous_chunk_center_x;
    int32_t previous_chunk_center_z;
    uint64_t chunk_unload_grace_until_ms;
    uint16_t chunk_scan_index;
    uint16_t chunk_sent_count;
    proto_chunk_coord_t sent_chunks[SERVER_CHUNK_TRACKED_MAX];    uint8_t selected_hotbar_slot;    uint16_t inventory_item_ids[46];
    uint8_t inventory_item_counts[46];
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

void proto_tick_server(uint64_t now_ms);
void proto_server_save_world(void);
bool proto_build_chat_packet(const char *message_text,
                             int64_t uuid_most,
                             int64_t uuid_least,
                             uint8_t *framed_packet_out,
                             size_t framed_packet_capacity,
                             size_t *framed_packet_length);

bool proto_send_player_presence(int socket_fd,
                                const proto_connection_t *player,
                                proto_send_callback_t send_fn,
                                void *send_context);

bool proto_send_player_remove(int socket_fd,
                              const proto_connection_t *player,
                              proto_send_callback_t send_fn,
                              void *send_context);

bool proto_send_entity_pos_rot(int socket_fd,
                               const proto_connection_t *player,
                               proto_send_callback_t send_fn,
                               void *send_context);

bool proto_send_entity_animation(int socket_fd,
                                 int32_t entity_id,
                                 uint8_t animation_id,
                                 proto_send_callback_t send_fn,
                                 void *send_context);

bool proto_send_health_update(int socket_fd,
                              const proto_connection_t *player,
                              proto_send_callback_t send_fn,
                              void *send_context);
