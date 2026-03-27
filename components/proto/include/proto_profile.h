#pragma once

#include <stdbool.h>
#include <stdint.h>

typedef struct
{
    uint16_t protocol_version;
    const char *display_name;

    int32_t handshake_next_state_status;
    int32_t handshake_next_state_login;

    int32_t c2s_handshake;
    int32_t c2s_status_request;
    int32_t c2s_status_ping;
    int32_t c2s_login_start;
    int32_t c2s_play_keepalive;
    int32_t c2s_play_chat;
    int32_t c2s_play_position;
    int32_t c2s_play_position_look;
    int32_t c2s_play_look;
    int32_t c2s_play_on_ground;
    int32_t c2s_play_block_dig;
    int32_t c2s_play_block_place;

    int32_t s2c_status_response;
    int32_t s2c_status_pong;
    int32_t s2c_login_disconnect;
    int32_t s2c_login_success;
    int32_t s2c_play_chat;
    int32_t s2c_play_block_change;
    int32_t s2c_play_chunk_data;
    int32_t s2c_play_update_light;
    int32_t s2c_play_unload_chunk;
    int32_t s2c_play_keepalive;
    int32_t s2c_play_join_game;
    int32_t s2c_play_player_position_look;
    int32_t s2c_play_update_view_position;
    int32_t s2c_play_update_health;

    bool supports_light_trust_edges;
} proto_profile_t;

const proto_profile_t *proto_profile_default(void);
const proto_profile_t *proto_profile_for_version(uint16_t protocol_version);
