#pragma once

#include <stdint.h>

typedef enum
{
    WORLD_BIOME_PLAINS = 0,
    WORLD_BIOME_FOREST = 1,
    WORLD_BIOME_DESERT = 2,
    WORLD_BIOME_SNOW = 3,
} world_biome_t;

typedef enum
{
    BLOCK_AIR = 0,
    BLOCK_STONE = 1,
    BLOCK_GRASS = 2,
    BLOCK_DIRT = 3,
    BLOCK_COBBLESTONE = 4,
    BLOCK_OAK_PLANKS = 5,
    BLOCK_SPRUCE_PLANKS = 6,
    BLOCK_WATER = 9,
    BLOCK_BEDROCK = 7,
    BLOCK_SAND = 12,
    BLOCK_OAK_LOG = 17,
    BLOCK_OAK_LEAVES = 18,
    BLOCK_DIAMOND_ORE = 56,
    BLOCK_CRAFTING_TABLE = 58,
    BLOCK_FURNACE = 61,
    BLOCK_SNOW_BLOCK = 80,
} world_block_id_t;

typedef struct
{
    uint32_t seed;
    int16_t min_y;
    int16_t max_y;
    int16_t sea_level;
    uint16_t biome_tile_size;
    uint8_t anchor_step;
} world_config_t;

void world_config_set_defaults(world_config_t *config, uint32_t seed);

world_biome_t world_query_biome(const world_config_t *config, int32_t x, int32_t z);
int16_t world_query_surface_y(const world_config_t *config, int32_t x, int32_t z);
uint8_t world_query_block(const world_config_t *config, int32_t x, int32_t y, int32_t z);
