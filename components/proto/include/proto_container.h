#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

#include "server_limits.h"
#include "proto_framing.h"

#define PROTO_CHEST_SLOTS 27
/* Open Window inventoryType for generic_9x3 (chest) in protocol 754.
 * Verified against the official 1.16.5 client MenuType registration order.
 */
#define PROTO_CHEST_WINDOW_TYPE 2
#define PROTO_CHEST_WINDOW_ID 1
#define PROTO_MAX_CHEST_INSTANCES 16

typedef struct {
    int32_t x;
    int32_t y;
    int32_t z;
    bool in_use;
    uint16_t item_ids[PROTO_CHEST_SLOTS];
    uint8_t item_counts[PROTO_CHEST_SLOTS];
} proto_container_state_t;

void proto_container_init(void);

proto_container_state_t *proto_container_find(int32_t x, int32_t y, int32_t z);
proto_container_state_t *proto_container_find_or_create(int32_t x, int32_t y, int32_t z);
void proto_container_free(int32_t x, int32_t y, int32_t z);

int16_t proto_container_window_slot_to_player_slot(int16_t window_slot);
bool proto_container_window_slot_is_chest(int16_t window_slot);
uint16_t *proto_container_resolve_item_ptr(proto_container_state_t *container,
                                            uint16_t *player_inventory,
                                            int16_t window_slot);
uint8_t *proto_container_resolve_count_ptr(proto_container_state_t *container,
                                            uint8_t *player_counts,
                                            int16_t window_slot);
