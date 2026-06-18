#include "proto_container.h"

#include <string.h>
#include "esp_log.h"

static const char *TAG = "proto_container";

static proto_container_state_t s_containers[PROTO_MAX_CHEST_INSTANCES];
static bool s_container_initialized = false;

static const int16_t PLAYER_TO_CHEST_WINDOW_OFFSET = 18;

void proto_container_init(void)
{
    if (!s_container_initialized)
    {
        memset(s_containers, 0, sizeof(s_containers));
        s_container_initialized = true;
    }
}

proto_container_state_t *proto_container_find(int32_t x, int32_t y, int32_t z)
{
    for (size_t i = 0; i < PROTO_MAX_CHEST_INSTANCES; i++)
    {
        if (s_containers[i].in_use &&
            s_containers[i].x == x &&
            s_containers[i].y == y &&
            s_containers[i].z == z)
        {
            return &s_containers[i];
        }
    }
    return NULL;
}

proto_container_state_t *proto_container_find_or_create(int32_t x, int32_t y, int32_t z)
{
    proto_container_state_t *existing = proto_container_find(x, y, z);
    if (existing != NULL)
    {
        return existing;
    }

    for (size_t i = 0; i < PROTO_MAX_CHEST_INSTANCES; i++)
    {
        if (!s_containers[i].in_use)
        {
            memset(&s_containers[i], 0, sizeof(s_containers[i]));
            s_containers[i].in_use = true;
            s_containers[i].x = x;
            s_containers[i].y = y;
            s_containers[i].z = z;
            return &s_containers[i];
        }
    }

    ESP_LOGW(TAG, "max chest instances reached (%u)", PROTO_MAX_CHEST_INSTANCES);
    return NULL;
}

void proto_container_free(int32_t x, int32_t y, int32_t z)
{
    proto_container_state_t *container = proto_container_find(x, y, z);
    if (container != NULL)
    {
        memset(container, 0, sizeof(*container));
    }
}

int16_t proto_container_window_slot_to_player_slot(int16_t window_slot)
{
    if (window_slot >= 27 && window_slot <= 62)
    {
        return (int16_t)(window_slot - PLAYER_TO_CHEST_WINDOW_OFFSET);
    }
    return -1;
}

bool proto_container_window_slot_is_chest(int16_t window_slot)
{
    return window_slot >= 0 && window_slot <= 26;
}

uint16_t *proto_container_resolve_item_ptr(proto_container_state_t *container,
                                            uint16_t *player_inventory,
                                            int16_t window_slot)
{
    if (container != NULL && proto_container_window_slot_is_chest(window_slot))
    {
        return &container->item_ids[window_slot];
    }
    int16_t player_idx = proto_container_window_slot_to_player_slot(window_slot);
    if (player_idx >= 9 && player_idx <= 44)
    {
        return &player_inventory[player_idx];
    }
    return NULL;
}

uint8_t *proto_container_resolve_count_ptr(proto_container_state_t *container,
                                            uint8_t *player_counts,
                                            int16_t window_slot)
{
    if (container != NULL && proto_container_window_slot_is_chest(window_slot))
    {
        return &container->item_counts[window_slot];
    }
    int16_t player_idx = proto_container_window_slot_to_player_slot(window_slot);
    if (player_idx >= 9 && player_idx <= 44)
    {
        return &player_counts[player_idx];
    }
    return NULL;
}
