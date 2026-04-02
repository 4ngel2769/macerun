#include "proto_profile.h"

#include "server_limits.h"

static const proto_profile_t PROFILE_754 = {
    .protocol_version = 754,
    .display_name = "1.16.5",

    .handshake_next_state_status = 1,
    .handshake_next_state_login = 2,

    .c2s_handshake = 0x00,
    .c2s_status_request = 0x00,
    .c2s_status_ping = 0x01,
    .c2s_login_start = 0x00,
    .c2s_play_keepalive = 0x10,
    .c2s_play_chat = 0x03,
    .c2s_play_position = 0x12,
    .c2s_play_position_look = 0x13,
    .c2s_play_look = 0x14,
    .c2s_play_on_ground = 0x15,
    .c2s_play_use_entity = 0x0E,
    .c2s_play_arm_animation = 0x2C,
    .c2s_play_block_dig = 0x1B,
    .c2s_play_block_place = 0x2E,
    .c2s_play_held_item_change = 0x25,
    .c2s_play_click_window = 0x09,

    .s2c_status_response = 0x00,
    .s2c_status_pong = 0x01,
    .s2c_login_disconnect = 0x00,
    .s2c_login_success = 0x02,
    .s2c_play_chat = 0x0E,
    .s2c_play_entity_animation = 0x05,
    .s2c_play_block_change = 0x0B,
    .s2c_play_chunk_data = 0x20,
    .s2c_play_update_light = 0x23,
    .s2c_play_unload_chunk = 0x1C,
    .s2c_play_keepalive = 0x1F,
    .s2c_play_join_game = 0x24,
    .s2c_play_player_position_look = 0x34,
    .s2c_play_update_view_position = 0x40,
    .s2c_play_set_slot = 0x15,
    .s2c_play_update_health = 0x49,

    .s2c_play_player_info      = 0x32,
    .s2c_play_spawn_player     = 0x04,
    .s2c_play_entity_position  = 0x27,
    .s2c_play_entity_pos_rot   = 0x28,
    .s2c_play_entity_rotation  = 0x29,
    .s2c_play_entity_head_look = 0x3A,
    .s2c_play_destroy_entities = 0x36,
    .s2c_play_ack_dig          = 0x07,

    .supports_light_trust_edges = true,
};

const proto_profile_t *proto_profile_default(void)
{
    return &PROFILE_754;
}

const proto_profile_t *proto_profile_for_version(uint16_t protocol_version)
{
    if (protocol_version == PROFILE_754.protocol_version)
    {
        return &PROFILE_754;
    }

    if (protocol_version == SERVER_PROTOCOL_VERSION)
    {
        return &PROFILE_754;
    }

    return &PROFILE_754;
}
