#pragma once

#include <stdint.h>
#include <stdbool.h>

#define PROTO_CRAFTING_GRID_SLOTS 9
#define PROTO_CRAFTING_TABLE_WINDOW_ID 2
/* Open Window inventoryType for the 3x3 crafting menu in protocol 754
 * ("crafting" = 11). Verified against the official 1.16.5 client. */
#define PROTO_CRAFTING_TABLE_WINDOW_TYPE 11

#define PROTO_ITEM_OAK_PLANKS 15
#define PROTO_ITEM_SPRUCE_PLANKS 16
#define PROTO_ITEM_BIRCH_PLANKS 17
#define PROTO_ITEM_JUNGLE_PLANKS 18
#define PROTO_ITEM_ACACIA_PLANKS 19
#define PROTO_ITEM_DARK_OAK_PLANKS 20
#define PROTO_ITEM_CRIMSON_PLANKS 21
#define PROTO_ITEM_WARPED_PLANKS 22
#define PROTO_ITEM_DIRT 9
#define PROTO_ITEM_COBBLESTONE 14
#define PROTO_ITEM_SAND 30
#define PROTO_ITEM_OAK_LOG 37
#define PROTO_ITEM_OAK_WOOD 61
#define PROTO_ITEM_STRIPPED_OAK_LOG 45
#define PROTO_ITEM_STRIPPED_OAK_WOOD 53
#define PROTO_ITEM_SPRUCE_LOG 38
#define PROTO_ITEM_SPRUCE_WOOD 62
#define PROTO_ITEM_STRIPPED_SPRUCE_LOG 46
#define PROTO_ITEM_STRIPPED_SPRUCE_WOOD 54
#define PROTO_ITEM_BAMBOO 135
#define PROTO_ITEM_OAK_LEAVES 69
#define PROTO_ITEM_CRAFTING_TABLE 183
#define PROTO_ITEM_FURNACE 185
#define PROTO_ITEM_DIAMOND 578
#define PROTO_ITEM_SNOWBALL 666
#define PROTO_ITEM_CHEST 180
#define PROTO_ITEM_STICK 613
#define PROTO_ITEM_WOODEN_SWORD 583
#define PROTO_ITEM_WOODEN_SPADE 584
#define PROTO_ITEM_WOODEN_PICKAXE 585
#define PROTO_ITEM_WOODEN_AXE 586
#define PROTO_ITEM_WOODEN_HOE 587
#define PROTO_ITEM_STONE_SWORD 588
#define PROTO_ITEM_STONE_SPADE 589
#define PROTO_ITEM_STONE_PICKAXE 590
#define PROTO_ITEM_STONE_AXE 591
#define PROTO_ITEM_STONE_HOE 592
#define PROTO_ITEM_IRON_SWORD 598
#define PROTO_ITEM_IRON_SPADE 599
#define PROTO_ITEM_IRON_PICKAXE 600
#define PROTO_ITEM_IRON_AXE 601
#define PROTO_ITEM_IRON_HOE 602
#define PROTO_ITEM_DIAMOND_SWORD 603
#define PROTO_ITEM_DIAMOND_SPADE 604
#define PROTO_ITEM_DIAMOND_PICKAXE 605
#define PROTO_ITEM_DIAMOND_AXE 606
#define PROTO_ITEM_DIAMOND_HOE 607
#define PROTO_ITEM_IRON_INGOT 579
#define PROTO_ITEM_GOLD_INGOT 580
#define PROTO_ITEM_COAL 576
#define PROTO_ITEM_OAK_STAIRS 179
#define PROTO_ITEM_OAK_SLAB 138
#define PROTO_ITEM_OAK_FENCE 208
#define PROTO_ITEM_OAK_FENCE_GATE 252
#define PROTO_ITEM_OAK_DOOR 558
#define PROTO_ITEM_OAK_TRAPDOOR 226
#define PROTO_ITEM_COBBLESTONE_STAIRS 188
#define PROTO_ITEM_COBBLESTONE_SLAB 151
#define PROTO_ITEM_COBBLESTONE_WALL 287
#define PROTO_ITEM_TORCH 171
#define PROTO_ITEM_LADDER 186
#define PROTO_ITEM_GLASS 77
#define PROTO_ITEM_GLASS_PANE 249
#define PROTO_ITEM_BOWL 614
#define PROTO_ITEM_BREAD 621
#define PROTO_ITEM_FEATHER 617

#define PROTO_CRAFT_TAG_PLANK 0xFFF1

typedef struct {
    uint8_t width;
    uint8_t height;
    uint16_t pattern[PROTO_CRAFTING_GRID_SLOTS];
    uint16_t output_item_id;
    uint8_t output_count;
} proto_table_shaped_recipe_t;

typedef struct {
    uint16_t item_id;
    uint8_t count;
    uint16_t consume_mask;
} proto_table_crafting_match_t;

bool proto_is_plank_item(uint16_t item_id);

proto_table_crafting_match_t proto_evaluate_table_crafting(const uint16_t *grid_items);

void proto_consume_table_ingredients(uint16_t *grid_items, uint8_t *grid_counts, uint16_t consume_mask);
