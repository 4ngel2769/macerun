#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "server_limits.h"

typedef struct __attribute__((packed))
{
    int16_t x;
    uint8_t y;
    int16_t z;
    uint8_t block_id;
} world_block_delta_t;

typedef struct
{
    world_block_delta_t entries[WORLD_MAX_BLOCK_DELTAS];
    size_t count;
} world_deltas_t;

void world_deltas_init(world_deltas_t *deltas);

bool world_deltas_get(const world_deltas_t *deltas,
                      int32_t x,
                      int32_t y,
                      int32_t z,
                      uint8_t *block_id_out);

bool world_deltas_put(world_deltas_t *deltas,
                      int32_t x,
                      int32_t y,
                      int32_t z,
                      uint8_t block_id);

bool world_deltas_remove(world_deltas_t *deltas,
                         int32_t x,
                         int32_t y,
                         int32_t z);
