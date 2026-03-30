#include "world_query.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "server_limits.h"

#define WORLD_TREE_CELL_SIZE 8
#define WORLD_DETAIL_STEP 16

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

static int32_t abs_i32(int32_t value)
{
    return value < 0 ? -value : value;
}

static int32_t interpolate_bilinear(int32_t h00,
                                    int32_t h10,
                                    int32_t h01,
                                    int32_t h11,
                                    int32_t tx,
                                    int32_t tz,
                                    int32_t step)
{
    int32_t top = h00 * (step - tx) + h10 * tx;
    int32_t bottom = h01 * (step - tx) + h11 * tx;
    return (top * (step - tz) + bottom * tz) / (step * step);
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
    int amplitude = 6;
    int factors = 4;

    switch (biome)
    {
    case WORLD_BIOME_PLAINS:
        base += 3;
        amplitude = 5;
        factors = 4;
        break;
    case WORLD_BIOME_FOREST:
        base += 4;
        amplitude = 9;
        factors = 4;
        break;
    case WORLD_BIOME_DESERT:
        base += 2;
        amplitude = 4;
        factors = 3;
        break;
    case WORLD_BIOME_SNOW:
        base += 6;
        amplitude = 11;
        factors = 3;
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

    int32_t blended = interpolate_bilinear(h00, h10, h01, h11, tx, tz, step);

    int32_t detail_cell_x = floor_div(x, WORLD_DETAIL_STEP);
    int32_t detail_cell_z = floor_div(z, WORLD_DETAIL_STEP);
    uint32_t detail_hash = hash_xz(config->seed ^ 0xA53C1E2Du,
                                   detail_cell_x,
                                   detail_cell_z,
                                   0x9E3779B9u);
    int32_t detail = ((int32_t)(detail_hash & 0x7u)) - 3;

    int32_t ridge_hash = (int32_t)((detail_hash >> 8) & 0x0Fu);
    if (ridge_hash < 3)
    {
        detail += 2;
    }

    blended += detail;

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

static bool biome_supports_trees(world_biome_t biome, uint32_t tree_hash)
{
    if (biome == WORLD_BIOME_FOREST)
    {
        return (tree_hash & 0x3u) == 0;
    }

    if (biome == WORLD_BIOME_PLAINS)
    {
        return (tree_hash & 0xFu) == 0;
    }

    return false;
}

static bool query_tree_block(const world_config_t *config,
                             int32_t x,
                             int32_t y,
                             int32_t z,
                             uint8_t *block_out)
{
    int32_t base_cell_x = floor_div(x, WORLD_TREE_CELL_SIZE);
    int32_t base_cell_z = floor_div(z, WORLD_TREE_CELL_SIZE);

    for (int32_t cell_dz = -1; cell_dz <= 1; cell_dz++)
    {
        for (int32_t cell_dx = -1; cell_dx <= 1; cell_dx++)
        {
            int32_t cell_x = base_cell_x + cell_dx;
            int32_t cell_z = base_cell_z + cell_dz;
            uint32_t tree_hash = hash_xz(config->seed ^ 0x7157A11u,
                                         cell_x,
                                         cell_z,
                                         0x54C3B17u);

            int32_t anchor_x = (cell_x * WORLD_TREE_CELL_SIZE) + (int32_t)(tree_hash & 0x7u);
            int32_t anchor_z = (cell_z * WORLD_TREE_CELL_SIZE) + (int32_t)((tree_hash >> 3) & 0x7u);

            world_biome_t anchor_biome = world_query_biome(config, anchor_x, anchor_z);
            if (!biome_supports_trees(anchor_biome, tree_hash >> 6))
            {
                continue;
            }

            int16_t anchor_surface = world_query_surface_y(config, anchor_x, anchor_z);
            if (anchor_surface <= config->sea_level)
            {
                continue;
            }

            int32_t trunk_base_y = anchor_surface + 1;
            int32_t trunk_height = 4 + (int32_t)((tree_hash >> 11) & 0x3u);
            int32_t trunk_top_y = trunk_base_y + trunk_height - 1;
            if (trunk_top_y + 2 > config->max_y)
            {
                continue;
            }

            int32_t dx = abs_i32(x - anchor_x);
            int32_t dz = abs_i32(z - anchor_z);

            if (x == anchor_x &&
                z == anchor_z &&
                y >= trunk_base_y &&
                y <= trunk_top_y)
            {
                *block_out = BLOCK_OAK_LOG;
                return true;
            }

            if (y < trunk_top_y - 2 || y > trunk_top_y + 2 || dx > 2 || dz > 2)
            {
                continue;
            }

            if (dx == 0 && dz == 0)
            {
                continue;
            }

            int32_t taxicab = dx + dz;
            if (y == trunk_top_y + 2)
            {
                if (taxicab > 1)
                {
                    continue;
                }
            }
            else if (y == trunk_top_y + 1)
            {
                if (taxicab > 2)
                {
                    continue;
                }
            }
            else if (y == trunk_top_y)
            {
                if (taxicab > 3)
                {
                    continue;
                }
            }
            else
            {
                if (dx == 2 && dz == 2)
                {
                    continue;
                }
            }

            *block_out = BLOCK_OAK_LEAVES;
            return true;
        }
    }

    return false;
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

        if (y <= surface_y + 8)
        {
            uint8_t tree_block = BLOCK_AIR;
            if (query_tree_block(config, x, y, z, &tree_block))
            {
                return tree_block;
            }
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

    int16_t cave_center = (int16_t)(config->sea_level - (surface_y - config->sea_level) - 14);
    uint32_t cave_hash = hash_xz(config->seed ^ 0x55AA55AAu,
                                 x,
                                 z,
                                 (uint32_t)(y * 31));
    uint32_t cave_hash_b = hash_xz(config->seed ^ 0x33CC44DDu,
                                   x,
                                   z,
                                   (uint32_t)(y * 17));
    bool carve = ((cave_hash & 0xFFu) < 22u) && ((cave_hash_b & 0x1Fu) < 4u);

    if (carve && y >= cave_center - 7 && y <= cave_center + 3)
    {
        return BLOCK_AIR;
    }

    uint32_t ore_seed = hash_xz(config->seed ^ 0x0DDC0FFEu, x, z, 0x1234ABCDu);
    int32_t ore_y = (int32_t)(ore_seed & 0x3Fu);
    if (y == ore_y && y < config->sea_level - 12)
    {
        uint32_t rarity = (ore_seed >> ((uint32_t)ore_y % 16u)) & 0xFFu;
        if (y < 14 && rarity < 10u)
        {
            return BLOCK_DIAMOND_ORE;
        }
    }

    return BLOCK_STONE;
}
