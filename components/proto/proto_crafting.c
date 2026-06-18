#include "proto_crafting.h"
#include "proto_profile.h"

#define CRAFT_COUNT (sizeof(s_recipes) / sizeof(s_recipes[0]))

#define _S PROTO_ITEM_STICK
#define _C PROTO_ITEM_COBBLESTONE
#define _D PROTO_ITEM_DIAMOND
#define _P PROTO_CRAFT_TAG_PLANK

#define _CP PROTO_ITEM_CHEST
#define _FU PROTO_ITEM_FURNACE
#define _WS PROTO_ITEM_WOODEN_SWORD
#define _WSP PROTO_ITEM_WOODEN_SPADE
#define _WP PROTO_ITEM_WOODEN_PICKAXE
#define _WA PROTO_ITEM_WOODEN_AXE
#define _WH PROTO_ITEM_WOODEN_HOE
#define _SS PROTO_ITEM_STONE_SWORD
#define _SSP PROTO_ITEM_STONE_SPADE
#define _SP PROTO_ITEM_STONE_PICKAXE
#define _SA PROTO_ITEM_STONE_AXE
#define _SH PROTO_ITEM_STONE_HOE
#define _DS PROTO_ITEM_DIAMOND_SWORD
#define _DSP PROTO_ITEM_DIAMOND_SPADE
#define _DP PROTO_ITEM_DIAMOND_PICKAXE
#define _DA PROTO_ITEM_DIAMOND_AXE
#define _DH PROTO_ITEM_DIAMOND_HOE
#define _BO PROTO_ITEM_BOWL
#define _LA PROTO_ITEM_LADDER
#define _TO PROTO_ITEM_TORCH

static const proto_table_shaped_recipe_t s_recipes[] = {

    {3, 3, {_P, _P, _P, _P, 0, _P, _P, _P, _P}, _CP, 1},
    {3, 3, {_C, _C, _C, _C, 0, _C, _C, _C, _C}, _FU, 1},

    {2, 2, {_P, _P, _P, _P}, _CP, 1},
    {2, 2, {_C, _C, _C, _C}, _FU, 1},

    {3, 3, {_P, _P, _P, 0, _S, 0, 0, _S, 0}, _WP, 1},
    {3, 3, {_P, _P, 0, _P, _S, 0, 0, _S, 0}, _WA, 1},
    {3, 1, {_P, _S, _S}, _WSP, 1},
    {3, 2, {_P, _P, 0, 0, _S, 0, 0, _S, 0}, _WH, 1},
    {2, 1, {_P, _S}, _WS, 1},

    {3, 3, {_C, _C, _C, 0, _S, 0, 0, _S, 0}, _SP, 1},
    {3, 3, {_C, _C, 0, _C, _S, 0, 0, _S, 0}, _SA, 1},
    {3, 1, {_C, _S, _S}, _SSP, 1},
    {3, 2, {_C, _C, 0, 0, _S, 0, 0, _S, 0}, _SH, 1},
    {2, 1, {_C, _S}, _SS, 1},

    {3, 3, {_D, _D, _D, 0, _S, 0, 0, _S, 0}, _DP, 1},
    {3, 3, {_D, _D, 0, _D, _S, 0, 0, _S, 0}, _DA, 1},
    {3, 1, {_D, _S, _S}, _DSP, 1},
    {3, 2, {_D, _D, 0, 0, _S, 0, 0, _S, 0}, _DH, 1},
    {2, 1, {_D, _S}, _DS, 1},

    {3, 2, {_P, _P, _P, _P, _P, _P}, _CP, 1},
};

bool proto_is_plank_item(uint16_t item_id)
{
    switch (item_id)
    {
    case PROTO_ITEM_OAK_PLANKS:
    case PROTO_ITEM_SPRUCE_PLANKS:
    case PROTO_ITEM_BIRCH_PLANKS:
    case PROTO_ITEM_JUNGLE_PLANKS:
    case PROTO_ITEM_ACACIA_PLANKS:
    case PROTO_ITEM_DARK_OAK_PLANKS:
    case PROTO_ITEM_CRIMSON_PLANKS:
    case PROTO_ITEM_WARPED_PLANKS:
        return true;
    default:
        return false;
    }
}

static bool item_matches_tag(uint16_t actual_item, uint16_t pattern_value)
{
    if (pattern_value == PROTO_CRAFT_TAG_PLANK)
    {
        return proto_is_plank_item(actual_item);
    }
    return actual_item == pattern_value;
}

static bool shaped_match_3x3(const uint16_t *grid_items,
                              const proto_table_shaped_recipe_t *recipe,
                              uint16_t *consume_mask_out)
{
    uint8_t max_offset_x = 3 - recipe->width;
    uint8_t max_offset_y = 3 - recipe->height;

    for (uint8_t offset_y = 0; offset_y <= max_offset_y; offset_y++)
    {
        for (uint8_t offset_x = 0; offset_x <= max_offset_x; offset_x++)
        {
            bool matches = true;
            uint16_t consume_mask = 0;

            for (uint8_t y = 0; y < 3 && matches; y++)
            {
                for (uint8_t x = 0; x < 3; x++)
                {
                    uint8_t grid_index = y * 3 + x;
                    uint16_t actual_item = grid_items[grid_index];

                    uint16_t expected_item = 0;
                    if (x >= offset_x && x < offset_x + recipe->width &&
                        y >= offset_y && y < offset_y + recipe->height)
                    {
                        uint8_t recipe_x = x - offset_x;
                        uint8_t recipe_y = y - offset_y;
                        uint8_t recipe_index = recipe_y * recipe->width + recipe_x;
                        expected_item = recipe->pattern[recipe_index];
                    }

                    if (!item_matches_tag(actual_item, expected_item))
                    {
                        matches = false;
                        break;
                    }

                    if (expected_item != 0)
                    {
                        consume_mask |= (uint16_t)(1u << grid_index);
                    }
                }
            }

            if (matches)
            {
                *consume_mask_out = consume_mask;
                return true;
            }
        }
    }

    return false;
}

proto_table_crafting_match_t proto_evaluate_table_crafting(const uint16_t *grid_items)
{
    proto_table_crafting_match_t match = {0};

    for (size_t i = 0; i < CRAFT_COUNT; i++)
    {
        uint16_t consume_mask = 0;
        if (shaped_match_3x3(grid_items, &s_recipes[i], &consume_mask))
        {
            match.item_id = s_recipes[i].output_item_id;
            match.count = s_recipes[i].output_count;
            match.consume_mask = consume_mask;
            return match;
        }
    }

    return match;
}

void proto_consume_table_ingredients(uint16_t *grid_items, uint8_t *grid_counts, uint16_t consume_mask)
{
    for (uint8_t i = 0; i < 9; i++)
    {
        if ((consume_mask & (uint16_t)(1u << i)) == 0)
        {
            continue;
        }
        if (grid_counts[i] == 0)
        {
            continue;
        }
        grid_counts[i]--;
        if (grid_counts[i] == 0)
        {
            grid_items[i] = 0;
        }
    }
}
