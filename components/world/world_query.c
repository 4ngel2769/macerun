#include "world_query.h"

#include <stdbool.h>
#include <stddef.h>

#include "server_limits.h"

static int32_t floor_div(int32_t value, int32_t divisor)
{
    int32_t result = value / divisor;
    int32_t rem = value % divisor;
    if (rem != 0 && ((rem < 0) != (divisor < 0)))
    {
        result--;
    }
    return result;
}

static uint32_t hash_mix(uint32_t value)
{
    value ^= value >> 16;
    value *= 0x7feb352d;
    value ^= value >> 15;
    value *= 0x846ca68b;
    value ^= value >> 16;
    return value;
}

static uint32_t hash_xz(uint32_t seed, int32_t x, int32_t z, uint32_t salt)
{
    uint32_t value = seed ^ salt;
    value ^= (uint32_t)x * 0x9e3779b9u;
    value ^= (uint32_t)z * 0x85ebca6bu;
    return hash_mix(value);
}

static int16_t clamp_i16(int32_t value, int16_t min_value, int16_t max_value)
{
    if (value < min_value)
    {
        return min_value;
    }
    if (value > max_value)
    {
        return max_value;
    }
    return (int16_t)value;
}

void world_config_set_defaults(world_config_t *config, uint32_t seed)
{
    config->seed = seed;
    config->min_y = WORLD_MIN_Y;
    config->max_y = WORLD_MAX_Y;
    config->sea_level = WORLD_SEA_LEVEL;
    config->biome_tile_size = 128;
    config->anchor_step = WORLD_ANCHOR_STEP;
}

world_biome_t world_query_biome(const world_config_t *config, int32_t x, int32_t z)
{
    int32_t cell_x = floor_div(x, config->biome_tile_size);
    int32_t cell_z = floor_div(z, config->biome_tile_size);
    uint32_t h = hash_xz(config->seed, cell_x, cell_z, 0xB16B00B5u);

    switch (h & 0x3u)
    {
    case 0:
        return WORLD_BIOME_PLAINS;
    case 1:
        return WORLD_BIOME_FOREST;
    case 2:
        return WORLD_BIOME_DESERT;
    default:
        return WORLD_BIOME_SNOW;
    }
}

static int16_t sample_anchor_height(const world_config_t *config, int32_t ax, int32_t az)
{
    world_biome_t biome = world_query_biome(config, ax, az);
    uint32_t h = hash_xz(config->seed, ax, az, 0xC001D00Du);

    int base = config->sea_level;
    int amplitude = 8;
    int factors = 3;

    switch (biome)
    {
    case WORLD_BIOME_PLAINS:
        base += 2;
        amplitude = 6;
        factors = 4;
        break;
    case WORLD_BIOME_FOREST:
        base += 3;
        amplitude = 10;
        factors = 3;
        break;
    case WORLD_BIOME_DESERT:
        base += 1;
        amplitude = 5;
        factors = 3;
        break;
    case WORLD_BIOME_SNOW:
        base += 4;
        amplitude = 14;
        factors = 2;
        break;
    }

    int sum = 0;
    for (int i = 0; i < factors; i++)
    {
        sum += (int)((h >> (i * 4)) & 0x0Fu);
    }

    int max_sum = factors * 15;
    int delta = ((sum * (2 * amplitude + 1)) / max_sum) - amplitude;
    int height = base + delta;

    if (biome == WORLD_BIOME_DESERT && height < config->sea_level)
    {
        height = config->sea_level;
    }

    return clamp_i16(height, config->min_y + 1, config->max_y);
}

int16_t world_query_surface_y(const world_config_t *config, int32_t x, int32_t z)
{
    int32_t step = config->anchor_step;
    int32_t cell_x = floor_div(x, step);
    int32_t cell_z = floor_div(z, step);

    int32_t x0 = cell_x * step;
    int32_t z0 = cell_z * step;
    int32_t x1 = x0 + step;
    int32_t z1 = z0 + step;

    int16_t h00 = sample_anchor_height(config, x0, z0);
    int16_t h10 = sample_anchor_height(config, x1, z0);
    int16_t h01 = sample_anchor_height(config, x0, z1);
    int16_t h11 = sample_anchor_height(config, x1, z1);

    int32_t tx = x - x0;
    int32_t tz = z - z0;

    int32_t top = h00 * (step - tx) + h10 * tx;
    int32_t bottom = h01 * (step - tx) + h11 * tx;
    int32_t blended = (top * (step - tz) + bottom * tz) / (step * step);

    return clamp_i16(blended, config->min_y + 1, config->max_y);
}

static uint8_t biome_surface_block(world_biome_t biome)
{
    switch (biome)
    {
    case WORLD_BIOME_DESERT:
        return BLOCK_SAND;
    case WORLD_BIOME_SNOW:
        return BLOCK_SNOW_BLOCK;
    case WORLD_BIOME_FOREST:
    case WORLD_BIOME_PLAINS:
    default:
        return BLOCK_GRASS;
    }
}

uint8_t world_query_block(const world_config_t *config, int32_t x, int32_t y, int32_t z)
{
    if (y < config->min_y || y > config->max_y)
    {
        return BLOCK_AIR;
    }

    if (y == config->min_y)
    {
        return BLOCK_BEDROCK;
    }

    world_biome_t biome = world_query_biome(config, x, z);
    int16_t surface_y = world_query_surface_y(config, x, z);

    if (y > surface_y)
    {
        if (y <= config->sea_level && biome != WORLD_BIOME_DESERT)
        {
            return BLOCK_WATER;
        }
        return BLOCK_AIR;
    }

    if (y == surface_y)
    {
        return biome_surface_block(biome);
    }

    if (y >= surface_y - 3)
    {
        return (biome == WORLD_BIOME_DESERT) ? BLOCK_SAND : BLOCK_DIRT;
    }

    int16_t cave_center = (int16_t)(config->sea_level - (surface_y - config->sea_level) - 12);
    uint32_t cave_hash = hash_xz(config->seed ^ 0x55AA55AAu, x, z, (uint32_t)y);
    bool carve = ((cave_hash & 0xFFu) < 20u);

    if (carve && y >= cave_center - 6 && y <= cave_center + 2)
    {
        return BLOCK_AIR;
    }

    uint32_t ore_hash = hash_xz(config->seed ^ 0x0DDC0FFEu, x, z, (uint32_t)(y * 13));
    if (y < config->sea_level - 24 && (ore_hash & 0x1FFu) == 0)
    {
        return BLOCK_DIAMOND_ORE;
    }

    return BLOCK_STONE;
}
