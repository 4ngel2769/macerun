#include "world_query.h"

#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "server_limits.h"

#define WORLD_TREE_CELL_SIZE 8

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

/* --- Gradient (Perlin) noise ------------------------------------------------ */
/* Smooth, deterministic 2D noise. hash_xz seeds a fixed set of gradients at
 * integer lattice points; fade() makes the interpolation continuous in both
 * value and slope, avoiding the square "grid" artefacts of value noise. */
static float noise_fade(float t)
{
    return t * t * t * (t * (t * 6.0f - 15.0f) + 10.0f);
}

static float noise_lerp(float a, float b, float t)
{
    return a + t * (b - a);
}

static float perlin_gradient(uint32_t seed, int32_t x, int32_t z, float dx, float dz)
{
    switch (hash_xz(seed, x, z, 0x5EED1234u) & 7u)
    {
    case 0: return  dx + dz;
    case 1: return -dx + dz;
    case 2: return  dx - dz;
    case 3: return -dx - dz;
    case 4: return  dx;
    case 5: return -dx;
    case 6: return  dz;
    default: return -dz;
    }
}

static float perlin2d(uint32_t seed, float x, float z)
{
    int32_t xi = (int32_t)floorf(x);
    int32_t zi = (int32_t)floorf(z);
    float dx = x - (float)xi;
    float dz = z - (float)zi;
    float u = noise_fade(dx);
    float v = noise_fade(dz);

    float n00 = perlin_gradient(seed, xi,     zi,     dx,        dz);
    float n10 = perlin_gradient(seed, xi + 1, zi,     dx - 1.0f, dz);
    float n01 = perlin_gradient(seed, xi,     zi + 1, dx,        dz - 1.0f);
    float n11 = perlin_gradient(seed, xi + 1, zi + 1, dx - 1.0f, dz - 1.0f);

    /* ~0.7 keeps the result in roughly [-1, 1] for the diagonal gradients. */
    return noise_lerp(noise_lerp(n00, n10, u),
                      noise_lerp(n01, n11, u), v) * 0.7f;
}

/* Fractal Brownian motion: stack octaves at doubling frequency / halving
 * amplitude so terrain has both broad continent shapes and small detail. */
static float fbm2d(uint32_t seed, float x, float z, int octaves)
{
    float amplitude = 1.0f;
    float frequency = 1.0f;
    float sum = 0.0f;
    float norm = 0.0f;

    for (int i = 0; i < octaves; i++)
    {
        sum += perlin2d(seed + (uint32_t)i * 0x9E3779B9u,
                        x * frequency, z * frequency) * amplitude;
        norm += amplitude;
        amplitude *= 0.5f;
        frequency *= 2.0f;
    }

    return sum / norm;
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
    /* Two smooth climate fields instead of one hash per 128-block tile:
     * temperature decides cold/hot, moisture decides forest/desert. This
     * produces contiguous, plausible biome regions with soft boundaries. */
    float temperature = fbm2d(config->seed ^ 0x0A11CEu,
                              x * 0.0035f, z * 0.0035f, 3);
    float moisture = fbm2d(config->seed ^ 0x0B0B1Eu,
                           x * 0.0045f, z * 0.0045f, 3);

    if (temperature < -0.28f)
    {
        return WORLD_BIOME_SNOW;
    }
    if (temperature > 0.18f && moisture < 0.05f)
    {
        return WORLD_BIOME_DESERT;
    }
    if (moisture > 0.12f)
    {
        return WORLD_BIOME_FOREST;
    }
    return WORLD_BIOME_PLAINS;
}

/* Terrain height from layered Perlin noise: a broad octave sets continent
 * scale hills, a detail octave adds gentle local variation. Biome sets the
 * base elevation and relief amplitude so deserts stay flatter and snowy
 * highlands rise above the surrounding land. */
static int16_t sample_surface_height(const world_config_t *config, int32_t x, int32_t z)
{
    world_biome_t biome = world_query_biome(config, x, z);

    int base = config->sea_level;
    float amplitude = 6.0f;

    switch (biome)
    {
    case WORLD_BIOME_PLAINS:
        base += 3;
        amplitude = 5.0f;
        break;
    case WORLD_BIOME_FOREST:
        base += 4;
        amplitude = 8.0f;
        break;
    case WORLD_BIOME_DESERT:
        base += 2;
        amplitude = 4.0f;
        break;
    case WORLD_BIOME_SNOW:
        base += 6;
        amplitude = 11.0f;
        break;
    }

    float continent = fbm2d(config->seed ^ 0xC0FFEEu,
                            x * 0.010f, z * 0.010f, 4);
    float detail = fbm2d(config->seed ^ 0xD00D00u,
                         x * 0.045f, z * 0.045f, 2);

    int height = base + (int)(continent * amplitude) + (int)(detail * 2.0f);

    if (biome == WORLD_BIOME_DESERT && height < config->sea_level)
    {
        height = config->sea_level;
    }

    return clamp_i16(height, config->min_y + 1, config->max_y);
}

int16_t world_query_surface_y(const world_config_t *config, int32_t x, int32_t z)
{
    return sample_surface_height(config, x, z);
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
