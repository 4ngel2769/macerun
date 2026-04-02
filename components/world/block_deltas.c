#include "block_deltas.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    int32_t x;
    int32_t y;
    int32_t z;
} search_key_t;

static int compare_deltas(const void *a, const void *b)
{
    const search_key_t *key = (const search_key_t *)a;
    const world_block_delta_t *entry = (const world_block_delta_t *)b;
    
    if (key->x != entry->x) return key->x < entry->x ? -1 : 1;
    if (key->y != entry->y) return key->y < entry->y ? -1 : 1;
    if (key->z != entry->z) return key->z < entry->z ? -1 : 1;
    
    return 0;
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
    search_key_t key = {x, y, z};
    world_block_delta_t *found = (world_block_delta_t *)bsearch(
        &key, 
        deltas->entries, 
        deltas->count, 
        sizeof(world_block_delta_t), 
        compare_deltas
    );
    
    if (found != NULL)
    {
        *block_id_out = found->block_id;
        return true;
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

    search_key_t key = {x, y, z};
    world_block_delta_t *found = (world_block_delta_t *)bsearch(
        &key, 
        deltas->entries, 
        deltas->count, 
        sizeof(world_block_delta_t), 
        compare_deltas
    );
    
    if (found != NULL)
    {
        found->block_id = block_id;
        return true;
    }

    if (deltas->count >= WORLD_MAX_BLOCK_DELTAS)
    {
        return false;
    }
    
    size_t insert_pos = 0;
    while (insert_pos < deltas->count)
    {
        if (compare_deltas(&key, &deltas->entries[insert_pos]) < 0)
        {
            break;
        }
        insert_pos++;
    }
    
    if (insert_pos < deltas->count)
    {
        memmove(
            &deltas->entries[insert_pos + 1],
            &deltas->entries[insert_pos],
            (deltas->count - insert_pos) * sizeof(world_block_delta_t)
        );
    }
    
    deltas->entries[insert_pos].x = (int16_t)x;
    deltas->entries[insert_pos].y = (uint8_t)y;
    deltas->entries[insert_pos].z = (int16_t)z;
    deltas->entries[insert_pos].block_id = block_id;
    deltas->count++;
    
    return true;
}

bool world_deltas_remove(world_deltas_t *deltas,
                         int32_t x,
                         int32_t y,
                         int32_t z)
{
    search_key_t key = {x, y, z};
    world_block_delta_t *found = (world_block_delta_t *)bsearch(
        &key, 
        deltas->entries, 
        deltas->count, 
        sizeof(world_block_delta_t), 
        compare_deltas
    );
    
    if (found != NULL)
    {
        size_t index = found - deltas->entries;
        size_t last = deltas->count - 1;
        
        if (index != last)
        {
            memmove(
                &deltas->entries[index],
                &deltas->entries[index + 1],
                (last - index) * sizeof(world_block_delta_t)
            );
        }
        
        deltas->count--;
        return true;
    }    
    return false;
}
