#include "proto_entity.h"

#include <math.h>
#include <stddef.h>
#include <string.h>

#include "esp_log.h"
#include "proto_framing.h"
#include "proto_profile.h"
#include "world_query.h"
#include "server_limits.h"

#define PROTO_ENTITY_TAG "proto_entity"
#define PROTO_ENTITY_VELOCITY_SCALE 8000

static const proto_profile_t *s_profile = NULL;
static world_config_t s_world_config;

static int32_t s_next_mob_entity_id = 1000;
static int64_t s_next_uuid_most = 0x7000000000000000LL;
static int64_t s_next_uuid_least = 0x0000000000000001LL;

static proto_mob_t s_mobs[PROTO_MAX_MOBS];

static uint64_t s_last_spawn_attempt_ms = 0;

static uint8_t s_proto_entity_buffer[512];

static int32_t next_mob_entity_id(void)
{
    int32_t id = s_next_mob_entity_id;
    s_next_mob_entity_id++;
    if (s_next_mob_entity_id >= 0x7FFFFFFF || s_next_mob_entity_id <= 0)
    {
        s_next_mob_entity_id = 1000;
    }
    return id;
}

static void generate_mob_uuid(int64_t *most, int64_t *least)
{
    *most = s_next_uuid_most;
    *least = s_next_uuid_least;
    s_next_uuid_least++;
    if (s_next_uuid_least <= 0)
    {
        s_next_uuid_least = 1;
        s_next_uuid_most++;
    }
}

static int32_t clamp_i32(int32_t value, int32_t min_val, int32_t max_val)
{
    if (value < min_val) return min_val;
    if (value > max_val) return max_val;
    return value;
}

static proto_mob_t *find_mob_by_id(int32_t entity_id)
{
    for (size_t i = 0; i < PROTO_MAX_MOBS; i++)
    {
        if (s_mobs[i].active && s_mobs[i].entity_id == entity_id)
        {
            return &s_mobs[i];
        }
    }
    return NULL;
}

static int find_free_mob_slot(void)
{
    for (size_t i = 0; i < PROTO_MAX_MOBS; i++)
    {
        if (!s_mobs[i].active)
        {
            return (int)i;
        }
    }
    return -1;
}

static bool is_block_solid(int32_t x, int32_t y, int32_t z)
{
    uint8_t block_id = world_query_block(&s_world_config, x, y, z);
    return block_id != 0;
}

static bool write_f64_be(proto_writer_t *writer, double value)
{
    union
    {
        double as_double;
        uint64_t as_u64;
    } converter = {.as_double = value};

    uint8_t bytes[8] = {
        (uint8_t)((converter.as_u64 >> 56) & 0xFF),
        (uint8_t)((converter.as_u64 >> 48) & 0xFF),
        (uint8_t)((converter.as_u64 >> 40) & 0xFF),
        (uint8_t)((converter.as_u64 >> 32) & 0xFF),
        (uint8_t)((converter.as_u64 >> 24) & 0xFF),
        (uint8_t)((converter.as_u64 >> 16) & 0xFF),
        (uint8_t)((converter.as_u64 >> 8) & 0xFF),
        (uint8_t)(converter.as_u64 & 0xFF),
    };
    return proto_write_bytes(writer, bytes, sizeof(bytes));
}

static bool write_i16_be(proto_writer_t *writer, int16_t value)
{
    uint16_t raw = (uint16_t)value;
    uint8_t bytes[2] = {
        (uint8_t)((raw >> 8) & 0xFF),
        (uint8_t)(raw & 0xFF),
    };
    return proto_write_bytes(writer, bytes, sizeof(bytes));
}

void proto_entity_init(void)
{
    memset(s_mobs, 0, sizeof(s_mobs));
    world_config_set_defaults(&s_world_config, WORLD_SEED_DEFAULT);
    s_profile = proto_profile_default();
    s_last_spawn_attempt_ms = 0;
}

int32_t proto_entity_spawn_mob(uint8_t entity_type,
                                double x,
                                double y,
                                double z)
{
    int slot = find_free_mob_slot();
    if (slot < 0)
    {
        return 0;
    }

    proto_mob_t *mob = &s_mobs[slot];

    mob->entity_id = next_mob_entity_id();
    generate_mob_uuid(&mob->uuid_most, &mob->uuid_least);
    mob->entity_type = entity_type;
    mob->active = true;
    mob->spawn_pending = true;

    mob->pos_x = x;
    mob->pos_y = y;
    mob->pos_z = z;
    mob->prev_pos_x = x;
    mob->prev_pos_y = y;
    mob->prev_pos_z = z;
    mob->yaw = 0.0f;
    mob->pitch = 0.0f;
    mob->velocity_x = 0.0f;
    mob->velocity_y = 0.0f;
    mob->velocity_z = 0.0f;
    mob->on_ground = false;

    mob->ai_state = MOB_STATE_IDLE;
    mob->next_action_ms = 0;
    mob->target_x = 0.0;
    mob->target_y = 0.0;
    mob->target_z = 0.0;
    mob->health = PROTO_MOB_MAX_HEALTH;
    mob->next_attack_ms = 0;

    return mob->entity_id;
}

void proto_entity_despawn_mob(int32_t entity_id)
{
    proto_mob_t *mob = find_mob_by_id(entity_id);
    if (mob != NULL)
    {
        mob->active = false;
    }
}

bool proto_entity_damage_mob(int32_t entity_id, float damage)
{
    proto_mob_t *mob = find_mob_by_id(entity_id);
    if (mob == NULL)
    {
        return false;
    }

    mob->health -= damage;
    if (mob->health < 0.0f)
    {
        mob->health = 0.0f;
    }

    return true;
}

static int count_active_mobs(void)
{
    int count = 0;
    for (size_t i = 0; i < PROTO_MAX_MOBS; i++)
    {
        if (s_mobs[i].active)
        {
            count++;
        }
    }
    return count;
}

static bool block_is_passable(int32_t x, int32_t y, int32_t z)
{
    return !is_block_solid(x, y, z);
}

static bool can_step_to(double x, double y, double z,
                         double dx, double dz)
{
    double nx = x + dx;
    double nz = z + dz;
    int32_t bx = (int32_t)floor(nx);
    int32_t by = (int32_t)floor(y);
    int32_t bz = (int32_t)floor(nz);

    bool feet_blocked = !block_is_passable(bx, by, bz);
    bool head_clear = block_is_passable(bx, by + 1, bz);

    if (!feet_blocked && head_clear)
    {
        return true;
    }

    if (feet_blocked && head_clear)
    {
        return block_is_passable(bx, by + 2, bz);
    }

    return false;
}

static void move_toward(proto_mob_t *mob,
                         double target_x,
                         double target_z,
                         float speed)
{
    double dx = target_x - mob->pos_x;
    double dz = target_z - mob->pos_z;
    double dist = sqrt(dx * dx + dz * dz);

    if (dist < 0.5)
    {
        mob->velocity_x = 0.0f;
        mob->velocity_z = 0.0f;
        return;
    }

    double dir_x = dx / dist;
    double dir_z = dz / dist;

    float move_x = (float)(dir_x * speed);
    float move_z = (float)(dir_z * speed);

    if (can_step_to(mob->pos_x, mob->pos_y, mob->pos_z,
                    move_x, 0.0))
    {
        mob->velocity_x = move_x;
        mob->velocity_z = move_z;
    }
    else if (can_step_to(mob->pos_x, mob->pos_y, mob->pos_z,
                         move_x, move_z))
    {
        mob->velocity_x = move_x;
        mob->velocity_z = move_z;
    }
    else
    {
        double strafe_x = -move_z;
        double strafe_z = move_x;
        if (can_step_to(mob->pos_x, mob->pos_y, mob->pos_z,
                        strafe_x, strafe_z))
        {
            mob->velocity_x = (float)strafe_x;
            mob->velocity_z = (float)strafe_z;
        }
        else if (can_step_to(mob->pos_x, mob->pos_y, mob->pos_z,
                             -strafe_x, -strafe_z))
        {
            mob->velocity_x = (float)(-strafe_x);
            mob->velocity_z = (float)(-strafe_z);
        }
        else
        {
            mob->velocity_x = 0.0f;
            mob->velocity_z = 0.0f;
        }
    }
}

static void apply_gravity_and_physics(proto_mob_t *mob)
{
    int32_t bx = (int32_t)floor(mob->pos_x);
    int32_t by = (int32_t)floor(mob->pos_y - 0.1);
    int32_t bz = (int32_t)floor(mob->pos_z);

    if (by < 0)
    {
        mob->on_ground = false;
    }
    else
    {
        mob->on_ground = is_block_solid(bx, by, bz);
    }

    if (mob->on_ground)
    {
        mob->velocity_y = 0.0f;
        mob->pos_y = (double)(by + 1);
    }
    else
    {
        mob->velocity_y += PROTO_MOB_GRAVITY;
        if (mob->velocity_y < -2.0f)
        {
            mob->velocity_y = -2.0f;
        }
    }

    bool can_step_up = false;
    if (mob->velocity_y == 0.0f && !mob->on_ground)
    {
        by = (int32_t)floor(mob->pos_y);
        if (by >= 0 && is_block_solid((int32_t)floor(mob->pos_x), by, (int32_t)floor(mob->pos_z)))
        {
            int32_t check_y = by + 1;
            if (block_is_passable((int32_t)floor(mob->pos_x), check_y, (int32_t)floor(mob->pos_z)) &&
                block_is_passable((int32_t)floor(mob->pos_x), check_y + 1, (int32_t)floor(mob->pos_z)))
            {
                mob->pos_y = (double)check_y;
                mob->velocity_y = 0.0f;
                mob->on_ground = true;
                can_step_up = true;
            }
        }
    }

    if (!can_step_up && mob->velocity_y == 0.0f && !mob->on_ground)
    {
        mob->velocity_y = PROTO_MOB_GRAVITY;
    }

    mob->prev_pos_x = mob->pos_x;
    mob->prev_pos_y = mob->pos_y;
    mob->prev_pos_z = mob->pos_z;

    mob->pos_x += (double)mob->velocity_x;
    mob->pos_y += (double)mob->velocity_y;
    mob->pos_z += (double)mob->velocity_z;

    double damp = 0.8;
    mob->velocity_x *= damp;
    mob->velocity_z *= damp;

    if (fabs(mob->velocity_x) < 0.001f)
    {
        mob->velocity_x = 0.0f;
    }
    if (fabs(mob->velocity_z) < 0.001f)
    {
        mob->velocity_z = 0.0f;
    }
}

static void pick_wander_target(proto_mob_t *mob)
{
    int32_t bx = (int32_t)floor(mob->pos_x);
    int32_t bz = (int32_t)floor(mob->pos_z);

    int32_t try_x = bx + (int32_t)(((int32_t)(mob->entity_id * 7 + 13)) % 17 - 8);
    int32_t try_z = bz + (int32_t)(((int32_t)(mob->entity_id * 11 + 37)) % 17 - 8);

    mob->target_x = (double)try_x + 0.5;
    mob->target_z = (double)try_z + 0.5;
    mob->target_y = 0.0;
}

static int find_nearest_player(const proto_mob_t *mob,
                                const proto_entity_player_view_t *players,
                                int player_count,
                                double *dist_out)
{
    int nearest = -1;
    double nearest_dist = PROTO_MOB_DETECTION_RANGE + 1.0;

    for (int i = 0; i < player_count; i++)
    {
        double dx = players[i].pos_x - mob->pos_x;
        double dz = players[i].pos_z - mob->pos_z;
        double dist = sqrt(dx * dx + dz * dz);

        if (dist < nearest_dist)
        {
            nearest_dist = dist;
            nearest = i;
        }
    }

    if (dist_out != NULL)
    {
        *dist_out = nearest_dist;
    }
    return nearest;
}

static void ai_tick_mob(proto_mob_t *mob,
                         uint64_t now_ms,
                         const proto_entity_player_view_t *players,
                         int player_count,
                         proto_entity_damage_fn damage_fn)
{
    if (!mob->active)
    {
        return;
    }

    double nearest_dist;
    int nearest_idx = find_nearest_player(mob, players, player_count, &nearest_dist);

    switch (mob->ai_state)
    {
    case MOB_STATE_IDLE:
        if (nearest_idx >= 0 && nearest_dist <= PROTO_MOB_DETECTION_RANGE)
        {
            mob->ai_state = MOB_STATE_CHASING;
            mob->next_action_ms = now_ms + 1000;
            mob->target_x = players[nearest_idx].pos_x;
            mob->target_z = players[nearest_idx].pos_z;
        }
        else if (now_ms >= mob->next_action_ms)
        {
            mob->ai_state = MOB_STATE_WANDERING;
            pick_wander_target(mob);
            mob->next_action_ms = now_ms + PROTO_MOB_WANDER_TO_NEW_MS;
        }
        break;

    case MOB_STATE_WANDERING:
        if (nearest_idx >= 0 && nearest_dist <= PROTO_MOB_DETECTION_RANGE)
        {
            mob->ai_state = MOB_STATE_CHASING;
            mob->next_action_ms = now_ms + 1000;
            mob->target_x = players[nearest_idx].pos_x;
            mob->target_z = players[nearest_idx].pos_z;
        }
        else if (now_ms >= mob->next_action_ms)
        {
            pick_wander_target(mob);
            mob->next_action_ms = now_ms + PROTO_MOB_WANDER_TO_NEW_MS;
        }

        move_toward(mob, mob->target_x, mob->target_z, PROTO_MOB_SPEED);
        break;

    case MOB_STATE_CHASING:
        if (nearest_idx < 0 || nearest_dist > PROTO_MOB_DETECTION_RANGE + 4.0f)
        {
            mob->ai_state = MOB_STATE_IDLE;
            mob->next_action_ms = now_ms + PROTO_MOB_IDLE_TO_WANDER_MS;
            mob->velocity_x = 0.0f;
            mob->velocity_z = 0.0f;
        }
        else
        {
            mob->target_x = players[nearest_idx].pos_x;
            mob->target_z = players[nearest_idx].pos_z;
            move_toward(mob, mob->target_x, mob->target_z, PROTO_MOB_SPEED * 1.3f);
        }
        break;
    }

    float dy = (float)(mob->target_z - mob->pos_z);
    float dx = (float)(mob->target_x - mob->pos_x);
    if (dx * dx + dy * dy > 0.01f)
    {
        mob->yaw = (float)(atan2(-dy, dx) * 180.0f / 3.14159265f);
    }

    if (mob->ai_state == MOB_STATE_CHASING && nearest_idx >= 0)
    {
        if (nearest_dist <= PROTO_MOB_ATTACK_RANGE &&
            now_ms >= mob->next_attack_ms)
        {
            mob->next_attack_ms = now_ms + PROTO_MOB_ATTACK_COOLDOWN_MS;
            if (damage_fn != NULL)
            {
                damage_fn(players[nearest_idx].entity_id,
                          PROTO_MOB_ATTACK_DAMAGE);
            }
        }
    }

    apply_gravity_and_physics(mob);
}

static void try_spawn_mob(uint64_t now_ms,
                           const proto_entity_player_view_t *players,
                           int player_count)
{
    if (now_ms - s_last_spawn_attempt_ms < PROTO_MOB_SPAWN_INTERVAL_MS)
    {
        return;
    }
    s_last_spawn_attempt_ms = now_ms;

    int active = count_active_mobs();
    if (active >= PROTO_MAX_MOBS)
    {
        return;
    }

    int max_new = 1;
    if (active + max_new > PROTO_MAX_MOBS)
    {
        max_new = PROTO_MAX_MOBS - active;
    }

    for (int n = 0; n < max_new; n++)
    {
        int32_t bx = 0;
        int32_t bz = 0;

        if (player_count > 0)
        {
            int pi = (int)((s_next_mob_entity_id + n) % player_count);
            int32_t px = (int32_t)floor(players[pi].pos_x);
            int32_t pz = (int32_t)floor(players[pi].pos_z);
            bx = px + (int32_t)(((int32_t)(s_next_mob_entity_id * 7 + 13 + n * 31)) % 17 - 8);
            bz = pz + (int32_t)(((int32_t)(s_next_mob_entity_id * 11 + 37 + n * 17)) % 17 - 8);
        }
        else
        {
            bx = (int32_t)((s_next_mob_entity_id * 7) % 21 - 10);
            bz = (int32_t)((s_next_mob_entity_id * 13) % 21 - 10);
        }

        int16_t sy = world_query_surface_y(&s_world_config, bx, bz);
        if (sy < s_world_config.min_y)
        {
            sy = s_world_config.sea_level;
        }

        double spawn_x = (double)bx + 0.5;
        double spawn_y = (double)sy + 1.0;
        double spawn_z = (double)bz + 0.5;

        int32_t id = proto_entity_spawn_mob(PROTO_ENTITY_TYPE_ZOMBIE,
                                             spawn_x, spawn_y, spawn_z);
        if (id != 0)
        {
            ESP_LOGI(PROTO_ENTITY_TAG,
                     "spawned zombie %ld at (%.1f, %.1f, %.1f)",
                     (long)id, spawn_x, spawn_y, spawn_z);
        }
    }
}

void proto_entity_tick(uint64_t now_ms,
                        const proto_entity_player_view_t *players,
                        int player_count,
                        proto_entity_damage_fn damage_fn)
{
    try_spawn_mob(now_ms, players, player_count);

    for (size_t i = 0; i < PROTO_MAX_MOBS; i++)
    {
        if (s_mobs[i].active)
        {
            ai_tick_mob(&s_mobs[i], now_ms, players, player_count, damage_fn);

            if (s_mobs[i].health <= 0.0f)
            {
                s_mobs[i].despawn_pending = true;
            }
        }
    }
}

static bool write_mob_spawn_packet(proto_writer_t *writer,
                                    const proto_mob_t *mob)
{
    uint8_t yaw_byte = (uint8_t)((int)(mob->yaw * 256.0f / 360.0f) & 0xFF);
    uint8_t pitch_byte = (uint8_t)((int)(mob->pitch * 256.0f / 360.0f) & 0xFF);
    int16_t vel_x = (int16_t)clamp_i32((int32_t)(mob->velocity_x * PROTO_ENTITY_VELOCITY_SCALE), -32768, 32767);
    int16_t vel_y = (int16_t)clamp_i32((int32_t)(mob->velocity_y * PROTO_ENTITY_VELOCITY_SCALE), -32768, 32767);
    int16_t vel_z = (int16_t)clamp_i32((int32_t)(mob->velocity_z * PROTO_ENTITY_VELOCITY_SCALE), -32768, 32767);

    return proto_write_varint(writer, s_profile->s2c_play_spawn_mob) &&
           proto_write_varint(writer, mob->entity_id) &&
           proto_write_i64_be(writer, mob->uuid_most) &&
           proto_write_i64_be(writer, mob->uuid_least) &&
           proto_write_varint(writer, (int32_t)mob->entity_type) &&
           write_f64_be(writer, mob->pos_x) &&
           write_f64_be(writer, mob->pos_y) &&
           write_f64_be(writer, mob->pos_z) &&
           proto_write_u8(writer, yaw_byte) &&
           proto_write_u8(writer, pitch_byte) &&
           proto_write_u8(writer, pitch_byte) &&
           write_i16_be(writer, vel_x) &&
           write_i16_be(writer, vel_y) &&
           write_i16_be(writer, vel_z);
}

void proto_entity_broadcast_updates(int player_socket_fds[],
                                     int player_count,
                                     proto_entity_send_fn send_fn,
                                     void *context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_entity_buffer, sizeof(s_proto_entity_buffer));

    for (size_t i = 0; i < PROTO_MAX_MOBS; i++)
    {
        proto_mob_t *mob = &s_mobs[i];
        if (!mob->active)
        {
            continue;
        }

        if (mob->spawn_pending)
        {
            writer.length = 0;
            if (!write_mob_spawn_packet(&writer, mob))
            {
                continue;
            }

            for (int j = 0; j < player_count; j++)
            {
                if (!send_fn(context, player_socket_fds[j],
                             s_proto_entity_buffer, writer.length))
                {
                    ESP_LOGW(PROTO_ENTITY_TAG,
                             "spawn packet send failed for socket %d",
                             player_socket_fds[j]);
                }
            }

            mob->spawn_pending = false;
        }
    }

    for (size_t i = 0; i < PROTO_MAX_MOBS; i++)
    {
        proto_mob_t *mob = &s_mobs[i];
        if (!mob->active || !mob->despawn_pending)
        {
            continue;
        }

        writer.length = 0;
        if (proto_write_varint(&writer, s_profile->s2c_play_destroy_entities) &&
            proto_write_varint(&writer, 1) &&
            proto_write_varint(&writer, mob->entity_id))
        {
            for (int j = 0; j < player_count; j++)
            {
                if (!send_fn(context, player_socket_fds[j],
                             s_proto_entity_buffer, writer.length))
                {
                    ESP_LOGW(PROTO_ENTITY_TAG,
                             "destroy packet send failed for socket %d",
                             player_socket_fds[j]);
                }
            }
        }

        mob->active = false;
        mob->despawn_pending = false;
    }

    for (size_t i = 0; i < PROTO_MAX_MOBS; i++)
    {
        proto_mob_t *mob = &s_mobs[i];
        if (!mob->active)
        {
            continue;
        }

        double delta_x = mob->pos_x - mob->prev_pos_x;
        double delta_y = mob->pos_y - mob->prev_pos_y;
        double delta_z = mob->pos_z - mob->prev_pos_z;
        bool moved = (delta_x * delta_x + delta_y * delta_y + delta_z * delta_z) > 1e-8;

        if (!moved)
        {
            continue;
        }

        int32_t enc_dx = (int32_t)(delta_x * 4096.0);
        int32_t enc_dy = (int32_t)(delta_y * 4096.0);
        int32_t enc_dz = (int32_t)(delta_z * 4096.0);

        enc_dx = clamp_i32(enc_dx, -32768, 32767);
        enc_dy = clamp_i32(enc_dy, -32768, 32767);
        enc_dz = clamp_i32(enc_dz, -32768, 32767);

        uint16_t raw_dx = (uint16_t)(int16_t)enc_dx;
        uint16_t raw_dy = (uint16_t)(int16_t)enc_dy;
        uint16_t raw_dz = (uint16_t)(int16_t)enc_dz;

        uint8_t delta_bytes[6] = {
            (uint8_t)((raw_dx >> 8) & 0xFF),
            (uint8_t)(raw_dx & 0xFF),
            (uint8_t)((raw_dy >> 8) & 0xFF),
            (uint8_t)(raw_dy & 0xFF),
            (uint8_t)((raw_dz >> 8) & 0xFF),
            (uint8_t)(raw_dz & 0xFF),
        };

        uint8_t yaw_byte = (uint8_t)((int)(mob->yaw * 256.0f / 360.0f) & 0xFF);
        uint8_t pitch_byte = (uint8_t)((int)(mob->pitch * 256.0f / 360.0f) & 0xFF);

        writer.length = 0;
        if (!proto_write_varint(&writer, s_profile->s2c_play_entity_pos_rot) ||
            !proto_write_varint(&writer, mob->entity_id) ||
            !proto_write_bytes(&writer, delta_bytes, 6) ||
            !proto_write_u8(&writer, yaw_byte) ||
            !proto_write_u8(&writer, pitch_byte) ||
            !proto_write_u8(&writer, mob->on_ground ? 1 : 0))
        {
            continue;
        }

        for (int j = 0; j < player_count; j++)
        {
            if (!send_fn(context, player_socket_fds[j],
                         s_proto_entity_buffer, writer.length))
            {
                ESP_LOGW(PROTO_ENTITY_TAG,
                         "move packet send failed for socket %d",
                         player_socket_fds[j]);
            }
        }

        mob->prev_pos_x = mob->pos_x;
        mob->prev_pos_y = mob->pos_y;
        mob->prev_pos_z = mob->pos_z;
    }
}

int proto_entity_get_active_count(void)
{
    return count_active_mobs();
}

const proto_mob_t *proto_entity_get_mobs(void)
{
    return s_mobs;
}
