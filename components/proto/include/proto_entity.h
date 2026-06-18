#pragma once

#include <stdbool.h>
#include <stdint.h>

#define PROTO_MAX_MOBS 16
#define PROTO_ENTITY_TYPE_ZOMBIE 32

#define PROTO_MOB_SPEED 0.23f
#define PROTO_MOB_DETECTION_RANGE 16.0f
#define PROTO_MOB_ATTACK_RANGE 2.0f
#define PROTO_MOB_WANDER_RANGE 8.0f
#define PROTO_MOB_GRAVITY -0.04f
#define PROTO_MOB_JUMP_VELOCITY 0.42f
#define PROTO_MOB_MAX_HEALTH 20.0f
#define PROTO_MOB_ATTACK_DAMAGE 3.0f
#define PROTO_MOB_ATTACK_COOLDOWN_MS 1000ULL
#define PROTO_MOB_SPAWN_INTERVAL_MS 5000ULL
#define PROTO_MOB_IDLE_TO_WANDER_MS 3000ULL
#define PROTO_MOB_WANDER_TO_NEW_MS 4000ULL
#define PROTO_MOB_MAX_SPAWN_DISTANCE 20.0f
#define PROTO_MOB_MIN_SPAWN_DISTANCE 8.0f
#define PROTO_MOB_PLAYER_DAMAGE 1.0f

typedef enum
{
    MOB_STATE_IDLE = 0,
    MOB_STATE_WANDERING,
    MOB_STATE_CHASING,
} proto_mob_state_t;

typedef struct
{
    int32_t entity_id;
    int64_t uuid_most;
    int64_t uuid_least;
    uint8_t entity_type;
    bool active;
    bool spawn_pending;
    bool despawn_pending;

    double pos_x;
    double pos_y;
    double pos_z;
    double prev_pos_x;
    double prev_pos_y;
    double prev_pos_z;
    float yaw;
    float pitch;
    float velocity_x;
    float velocity_y;
    float velocity_z;
    bool on_ground;

    proto_mob_state_t ai_state;
    uint64_t next_action_ms;
    double target_x;
    double target_y;
    double target_z;

    float health;
    uint64_t next_attack_ms;
} proto_mob_t;

typedef struct
{
    int32_t entity_id;
    double pos_x;
    double pos_y;
    double pos_z;
} proto_entity_player_view_t;

typedef void (*proto_entity_damage_fn)(int32_t target_player_entity_id,
                                        float damage);

void proto_entity_init(void);

int32_t proto_entity_spawn_mob(uint8_t entity_type,
                               double x,
                               double y,
                               double z);

void proto_entity_despawn_mob(int32_t entity_id);

bool proto_entity_damage_mob(int32_t entity_id, float damage);

void proto_entity_tick(uint64_t now_ms,
                       const proto_entity_player_view_t *players,
                       int player_count,
                       proto_entity_damage_fn damage_fn);

typedef bool (*proto_entity_send_fn)(void *context,
                                     int socket_fd,
                                     const uint8_t *data,
                                     size_t length);

void proto_entity_broadcast_updates(int player_socket_fds[],
                                    int player_count,
                                    proto_entity_send_fn send_fn,
                                    void *context);
