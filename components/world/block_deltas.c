#include "block_deltas.h"

#include <limits.h>
#include <string.h>

static bool coords_match(const world_block_delta_t *entry, int32_t x, int32_t y, int32_t z)
{
    return entry->x == x && entry->y == y && entry->z == z;
}

void world_deltas_init(world_deltas_t *deltas)
{
    memset(deltas, 0, sizeof(*deltas));
}

bool world_deltas_get(const world_deltas_t *deltas,
                      int32_t x,
                      int32_t y,
                      int32_t z,
                      uint8_t *block_id_out)
{
    for (size_t i = 0; i < deltas->count; i++)
    {
        if (coords_match(&deltas->entries[i], x, y, z))
        {
            *block_id_out = deltas->entries[i].block_id;
            return true;
        }
    }

    return false;
}

bool world_deltas_put(world_deltas_t *deltas,
                      int32_t x,
                      int32_t y,
                      int32_t z,
                      uint8_t block_id)
{
    if (y < 0 || y > UINT8_MAX || x < INT16_MIN || x > INT16_MAX || z < INT16_MIN || z > INT16_MAX)
    {
        return false;
    }

    for (size_t i = 0; i < deltas->count; i++)
    {
        if (coords_match(&deltas->entries[i], x, y, z))
        {
            deltas->entries[i].block_id = block_id;
            return true;
        }
    }

    if (deltas->count >= WORLD_MAX_BLOCK_DELTAS)
    {
        return false;
    }

    world_block_delta_t *entry = &deltas->entries[deltas->count++];
    entry->x = (int16_t)x;
    entry->y = (uint8_t)y;
    entry->z = (int16_t)z;
    entry->block_id = block_id;
    return true;
}

bool world_deltas_remove(world_deltas_t *deltas,
                         int32_t x,
                         int32_t y,
                         int32_t z)
{
    for (size_t i = 0; i < deltas->count; i++)
    {
        if (coords_match(&deltas->entries[i], x, y, z))
        {
            size_t last = deltas->count - 1;
            if (i != last)
            {
                deltas->entries[i] = deltas->entries[last];
            }
            deltas->count--;
            return true;
        }
    }

    return false;
}
