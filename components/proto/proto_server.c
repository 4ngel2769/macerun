#include "proto_server.h"

#include <math.h>
#include <limits.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "esp_err.h"
#include "esp_log.h"
#include "nvs.h"
#include "proto_framing.h"
#include "server_limits.h"
#include "block_deltas.h"
#include "world_query.h"

#define PROTO_KEEPALIVE_INTERVAL_MS 5000ULL
#define PROTO_KEEPALIVE_TIMEOUT_MS 15000ULL
#define PROTO_HUNGER_DECAY_INTERVAL_MS 30000ULL
#define PROTO_HEALTH_REGEN_INTERVAL_MS 5000ULL
#define PROTO_STARVATION_DAMAGE_INTERVAL_MS 3000ULL
#define PROTO_VOID_DAMAGE_INTERVAL_MS 600ULL

typedef enum
{
    NBT_TAG_END = 0,
    NBT_TAG_BYTE = 1,
    NBT_TAG_INT = 3,
    NBT_TAG_LONG = 4,
    NBT_TAG_FLOAT = 5,
    NBT_TAG_DOUBLE = 6,
    NBT_TAG_STRING = 8,
    NBT_TAG_LIST = 9,
    NBT_TAG_COMPOUND = 10,
    NBT_TAG_LONG_ARRAY = 12,
} nbt_tag_t;

#define PROTO_SECTION_WIDTH 16
#define PROTO_SECTION_HEIGHT 16
#define PROTO_SECTION_VOLUME (PROTO_SECTION_WIDTH * PROTO_SECTION_WIDTH * PROTO_SECTION_HEIGHT)
#define PROTO_SECTION_COUNT 16
#define PROTO_LIGHT_SECTION_COUNT (PROTO_SECTION_COUNT + 2)
#define PROTO_HEIGHTMAP_ENTRY_COUNT 256
#define PROTO_HEIGHTMAP_BITS_PER_ENTRY 9
#define PROTO_HEIGHTMAP_LONG_COUNT 36
#define PROTO_BIOME_ARRAY_COUNT (4 * 4 * 64)
#define PROTO_CHUNK_LIGHT_EMPTY_MASK ((1 << PROTO_LIGHT_SECTION_COUNT) - 1)
#define PROTO_CHUNK_BITS_PER_BLOCK 4
#define PROTO_WORLD_NVS_NAMESPACE "macerun"
#define PROTO_WORLD_NVS_KEY "world_deltas"
#define PROTO_KEEPALIVE_RETRY_DELAY_MS 1000ULL
#define PROTO_CHUNK_UNLOAD_GRACE_MS 2000ULL
#define PROTO_VOID_RECOVERY_THRESHOLD_BELOW_MIN_Y 16.0
#define PROTO_MAX_Y_CLAMP_MARGIN 64.0
#define PROTO_RECOVERY_SURFACE_OFFSET 2.0

static int32_t s_next_entity_id = 1;
static bool s_world_initialized = false;
static world_config_t s_world_config;
static world_deltas_t s_world_deltas;
static const char *TAG = "proto_server";

static uint8_t s_proto_packet_buffer[SERVER_MAX_OUTBOUND_PACKET_SIZE];
static uint8_t s_proto_framed_buffer[SERVER_MAX_OUTBOUND_PACKET_SIZE + 8];
static uint8_t s_proto_chunk_data_buffer[SERVER_MAX_CHUNK_DATA_SIZE];

static const int32_t s_chunk_palette_state_ids[] = {
    0,
    1,
    9,
    10,
    14,
    33,
    34,
    66,
    74,
    158,
    3354,
    3930,
};

#define PROTO_CHUNK_PALETTE_SIZE ((int32_t)(sizeof(s_chunk_palette_state_ids) / sizeof(s_chunk_palette_state_ids[0])))

static bool send_packet(int socket_fd,
                        const uint8_t *packet_body,
                        size_t packet_length,
                        proto_send_callback_t send_fn,
                        void *send_context);

static bool broadcast_packet(int source_socket_fd,
                             const uint8_t *packet_body,
                             size_t packet_length,
                             proto_broadcast_callback_t broadcast_fn,
                             void *send_context);

static bool load_world_deltas_from_nvs(void);
static bool persist_world_deltas_to_nvs(void);
static void initialize_survival_state(proto_connection_t *connection, uint64_t now_ms);

static void ensure_world_initialized(void)
{
    if (s_world_initialized)
    {
        return;
    }

    world_config_set_defaults(&s_world_config, WORLD_SEED_DEFAULT);
    world_deltas_init(&s_world_deltas);
    if (!load_world_deltas_from_nvs())
    {
        ESP_LOGW(TAG, "world delta restore failed; starting with empty overrides");
    }
    s_world_initialized = true;
}

static bool write_i32_be(proto_writer_t *writer, int32_t value)
{
    uint32_t unsigned_value = (uint32_t)value;
    uint8_t bytes[4] = {
        (uint8_t)((unsigned_value >> 24) & 0xFF),
        (uint8_t)((unsigned_value >> 16) & 0xFF),
        (uint8_t)((unsigned_value >> 8) & 0xFF),
        (uint8_t)(unsigned_value & 0xFF),
    };

    return proto_write_bytes(writer, bytes, sizeof(bytes));
}

static bool write_f32_be(proto_writer_t *writer, float value)
{
    union
    {
        float as_float;
        uint32_t as_u32;
    } converter = {.as_float = value};

    uint8_t bytes[4] = {
        (uint8_t)((converter.as_u32 >> 24) & 0xFF),
        (uint8_t)((converter.as_u32 >> 16) & 0xFF),
        (uint8_t)((converter.as_u32 >> 8) & 0xFF),
        (uint8_t)(converter.as_u32 & 0xFF),
    };

    return proto_write_bytes(writer, bytes, sizeof(bytes));
}

static bool write_f64_be(proto_writer_t *writer, double value)
{
    union
    {
        double as_double;
        uint64_t as_u64;
    } converter = {.as_double = value};

    uint8_t bytes[8] = {
        (uint8_t)((converter.as_u64 >> 56) & 0xFF),
        (uint8_t)((converter.as_u64 >> 48) & 0xFF),
        (uint8_t)((converter.as_u64 >> 40) & 0xFF),
        (uint8_t)((converter.as_u64 >> 32) & 0xFF),
        (uint8_t)((converter.as_u64 >> 24) & 0xFF),
        (uint8_t)((converter.as_u64 >> 16) & 0xFF),
        (uint8_t)((converter.as_u64 >> 8) & 0xFF),
        (uint8_t)(converter.as_u64 & 0xFF),
    };

    return proto_write_bytes(writer, bytes, sizeof(bytes));
}

static bool read_bool(proto_reader_t *reader, bool *value)
{
    uint8_t raw = 0;
    if (!proto_read_u8(reader, &raw))
    {
        return false;
    }

    *value = raw != 0;
    return true;
}

static bool read_f32_be(proto_reader_t *reader, float *value)
{
    uint8_t bytes[4] = {0};
    for (size_t i = 0; i < sizeof(bytes); i++)
    {
        if (!proto_read_u8(reader, &bytes[i]))
        {
            return false;
        }
    }

    union
    {
        uint32_t as_u32;
        float as_float;
    } converter = {
        .as_u32 = ((uint32_t)bytes[0] << 24) |
                  ((uint32_t)bytes[1] << 16) |
                  ((uint32_t)bytes[2] << 8) |
                  (uint32_t)bytes[3],
    };

    *value = converter.as_float;
    return true;
}

static bool read_f64_be(proto_reader_t *reader, double *value)
{
    uint8_t bytes[8] = {0};
    for (size_t i = 0; i < sizeof(bytes); i++)
    {
        if (!proto_read_u8(reader, &bytes[i]))
        {
            return false;
        }
    }

    union
    {
        uint64_t as_u64;
        double as_double;
    } converter = {
        .as_u64 = ((uint64_t)bytes[0] << 56) |
                  ((uint64_t)bytes[1] << 48) |
                  ((uint64_t)bytes[2] << 40) |
                  ((uint64_t)bytes[3] << 32) |
                  ((uint64_t)bytes[4] << 24) |
                  ((uint64_t)bytes[5] << 16) |
                  ((uint64_t)bytes[6] << 8) |
                  (uint64_t)bytes[7],
    };

    *value = converter.as_double;
    return true;
}

static uint64_t fnv1a64_seeded(const char *text, uint64_t seed)
{
    uint64_t hash = seed;
    while (*text != '\0')
    {
        hash ^= (uint8_t)(*text);
        hash *= 1099511628211ULL;
        text++;
    }
    return hash;
}

static int64_t be_bytes_to_i64(const uint8_t *bytes)
{
    uint64_t unsigned_value = 0;
    for (size_t i = 0; i < 8; i++)
    {
        unsigned_value = (unsigned_value << 8) | (uint64_t)bytes[i];
    }
    return (int64_t)unsigned_value;
}

static void generate_offline_uuid(const char *username, int64_t *uuid_most, int64_t *uuid_least)
{
    uint64_t high_half = fnv1a64_seeded(username, 1469598103934665603ULL);
    uint64_t low_half = fnv1a64_seeded(username, 1099511628211ULL ^ 0x6A09E667F3BCC909ULL);

    uint8_t bytes[16];
    for (size_t index = 0; index < 8; index++)
    {
        bytes[index] = (uint8_t)((high_half >> (56 - (8 * index))) & 0xFF);
        bytes[8 + index] = (uint8_t)((low_half >> (56 - (8 * index))) & 0xFF);
    }

    bytes[6] = (uint8_t)((bytes[6] & 0x0F) | 0x30);
    bytes[8] = (uint8_t)((bytes[8] & 0x3F) | 0x80);

    *uuid_most = be_bytes_to_i64(bytes);
    *uuid_least = be_bytes_to_i64(bytes + 8);
}

static int32_t next_entity_id(void)
{
    int32_t entity_id = s_next_entity_id;
    s_next_entity_id++;
    if (s_next_entity_id <= 0)
    {
        s_next_entity_id = 1;
    }
    return entity_id;
}

static bool escape_json_text(const char *input, char *output, size_t output_size)
{
    size_t out_index = 0;

    for (size_t in_index = 0; input[in_index] != '\0'; in_index++)
    {
        char replacement[3] = {0};
        const char *fragment = NULL;

        if (input[in_index] == '"')
        {
            fragment = "\\\"";
        }
        else if (input[in_index] == '\\')
        {
            fragment = "\\\\";
        }
        else if (input[in_index] == '\n')
        {
            fragment = "\\n";
        }
        else if (input[in_index] == '\r')
        {
            fragment = "\\r";
        }
        else if (input[in_index] == '\t')
        {
            fragment = "\\t";
        }
        else if ((uint8_t)input[in_index] < 0x20)
        {
            replacement[0] = ' ';
            fragment = replacement;
        }
        else
        {
            replacement[0] = input[in_index];
            fragment = replacement;
        }

        size_t fragment_length = strlen(fragment);
        if (out_index + fragment_length + 1 > output_size)
        {
            return false;
        }

        memcpy(output + out_index, fragment, fragment_length);
        out_index += fragment_length;
    }

    if (out_index >= output_size)
    {
        return false;
    }

    output[out_index] = '\0';
    return true;
}

static bool nbt_write_string(proto_writer_t *writer, const char *value)
{
    size_t length = strlen(value);
    if (length > 0xFFFFu)
    {
        return false;
    }

    return proto_write_u16_be(writer, (uint16_t)length) &&
           proto_write_bytes(writer, (const uint8_t *)value, length);
}

static bool nbt_write_named_header(proto_writer_t *writer, nbt_tag_t tag_type, const char *name)
{
    return proto_write_u8(writer, (uint8_t)tag_type) &&
           nbt_write_string(writer, name);
}

static bool nbt_begin_named_compound(proto_writer_t *writer, const char *name)
{
    return nbt_write_named_header(writer, NBT_TAG_COMPOUND, name);
}

static bool nbt_end_compound(proto_writer_t *writer)
{
    return proto_write_u8(writer, NBT_TAG_END);
}

static bool nbt_begin_named_list(proto_writer_t *writer,
                                 const char *name,
                                 nbt_tag_t element_type,
                                 int32_t count)
{
    return nbt_write_named_header(writer, NBT_TAG_LIST, name) &&
           proto_write_u8(writer, (uint8_t)element_type) &&
           write_i32_be(writer, count);
}

static bool nbt_write_named_byte(proto_writer_t *writer, const char *name, uint8_t value)
{
    return nbt_write_named_header(writer, NBT_TAG_BYTE, name) &&
           proto_write_u8(writer, value);
}

static bool nbt_write_named_int(proto_writer_t *writer, const char *name, int32_t value)
{
    return nbt_write_named_header(writer, NBT_TAG_INT, name) &&
           write_i32_be(writer, value);
}

static bool nbt_write_named_float(proto_writer_t *writer, const char *name, float value)
{
    return nbt_write_named_header(writer, NBT_TAG_FLOAT, name) &&
           write_f32_be(writer, value);
}

static bool nbt_write_named_double(proto_writer_t *writer, const char *name, double value)
{
    return nbt_write_named_header(writer, NBT_TAG_DOUBLE, name) &&
           write_f64_be(writer, value);
}

static bool nbt_write_named_string(proto_writer_t *writer, const char *name, const char *value)
{
    return nbt_write_named_header(writer, NBT_TAG_STRING, name) &&
           nbt_write_string(writer, value);
}

static bool nbt_write_named_long_array(proto_writer_t *writer,
                                       const char *name,
                                       const int64_t *values,
                                       int32_t count)
{
    if (count < 0)
    {
        return false;
    }

    if (!nbt_write_named_header(writer, NBT_TAG_LONG_ARRAY, name) ||
        !write_i32_be(writer, count))
    {
        return false;
    }

    for (int32_t i = 0; i < count; i++)
    {
        if (!proto_write_i64_be(writer, values[i]))
        {
            return false;
        }
    }

    return true;
}

static int32_t world_block_to_state_id(uint8_t block_id)
{
    switch (block_id)
    {
    case BLOCK_AIR:
        return 0;
    case BLOCK_STONE:
        return 1;
    case BLOCK_GRASS:
        return 9;
    case BLOCK_DIRT:
        return 10;
    case BLOCK_COBBLESTONE:
        return 14;
    case BLOCK_BEDROCK:
        return 33;
    case BLOCK_WATER:
        return 34;
    case BLOCK_SAND:
        return 66;
    case BLOCK_OAK_LOG:
        return 74;
    case BLOCK_OAK_LEAVES:
        return 158;
    case BLOCK_DIAMOND_ORE:
        return 3354;
    case BLOCK_SNOW_BLOCK:
        return 3930;
    default:
        return 1;
    }
}

static uint8_t world_block_to_palette_index(uint8_t block_id)
{
    switch (block_id)
    {
    case BLOCK_AIR:
        return 0;
    case BLOCK_STONE:
        return 1;
    case BLOCK_GRASS:
        return 2;
    case BLOCK_DIRT:
        return 3;
    case BLOCK_COBBLESTONE:
        return 4;
    case BLOCK_BEDROCK:
        return 5;
    case BLOCK_WATER:
        return 6;
    case BLOCK_SAND:
        return 7;
    case BLOCK_OAK_LOG:
        return 8;
    case BLOCK_OAK_LEAVES:
        return 9;
    case BLOCK_DIAMOND_ORE:
        return 10;
    case BLOCK_SNOW_BLOCK:
        return 11;
    default:
        return 1;
    }
}

static uint8_t query_block_id(int32_t x, int32_t y, int32_t z)
{
    ensure_world_initialized();

    uint8_t block_id = 0;
    if (world_deltas_get(&s_world_deltas, x, y, z, &block_id))
    {
        return block_id;
    }

    return world_query_block(&s_world_config, x, y, z);
}

static int32_t sign_extend_i32(uint32_t value, uint8_t width_bits)
{
    if (width_bits == 0 || width_bits >= 32)
    {
        return (int32_t)value;
    }

    uint32_t shift = 32u - (uint32_t)width_bits;
    return ((int32_t)(value << shift)) >> shift;
}

static bool read_block_position(proto_reader_t *reader,
                                int32_t *x_out,
                                int32_t *y_out,
                                int32_t *z_out)
{
    int64_t packed_signed = 0;
    if (!proto_read_i64_be(reader, &packed_signed))
    {
        return false;
    }

    uint64_t packed = (uint64_t)packed_signed;
    uint32_t raw_x = (uint32_t)((packed >> 38) & 0x3FFFFFFULL);
    uint32_t raw_y = (uint32_t)(packed & 0xFFFULL);
    uint32_t raw_z = (uint32_t)((packed >> 12) & 0x3FFFFFFULL);

    *x_out = sign_extend_i32(raw_x, 26);
    *y_out = sign_extend_i32(raw_y, 12);
    *z_out = sign_extend_i32(raw_z, 26);
    return true;
}

static bool write_block_position(proto_writer_t *writer,
                                 int32_t x,
                                 int32_t y,
                                 int32_t z)
{
    uint64_t packed = (((uint64_t)x & 0x3FFFFFFULL) << 38) |
                      (((uint64_t)z & 0x3FFFFFFULL) << 12) |
                      ((uint64_t)y & 0xFFFULL);
    return proto_write_i64_be(writer, (int64_t)packed);
}

static bool load_world_deltas_from_nvs(void)
{
    nvs_handle_t handle = 0;
    esp_err_t err = nvs_open(PROTO_WORLD_NVS_NAMESPACE, NVS_READONLY, &handle);
    if (err == ESP_ERR_NVS_NOT_FOUND)
    {
        return true;
    }

    if (err != ESP_OK)
    {
        ESP_LOGW(TAG, "nvs_open(read) failed: %s", esp_err_to_name(err));
        return false;
    }

    size_t blob_size = 0;
    err = nvs_get_blob(handle, PROTO_WORLD_NVS_KEY, NULL, &blob_size);
    if (err == ESP_ERR_NVS_NOT_FOUND)
    {
        nvs_close(handle);
        return true;
    }

    if (err != ESP_OK)
    {
        ESP_LOGW(TAG, "nvs_get_blob(size) failed: %s", esp_err_to_name(err));
        nvs_close(handle);
        return false;
    }

    if (blob_size == 0)
    {
        nvs_close(handle);
        return true;
    }

    void *blob_data = malloc(blob_size);
    if (blob_data == NULL)
    {
        ESP_LOGW(TAG, "world delta restore failed: alloc %u bytes", (unsigned int)blob_size);
        nvs_close(handle);
        return false;
    }

    size_t read_size = blob_size;
    err = nvs_get_blob(handle, PROTO_WORLD_NVS_KEY, blob_data, &read_size);
    nvs_close(handle);
    if (err != ESP_OK)
    {
        ESP_LOGW(TAG, "nvs_get_blob(data) failed: %s", esp_err_to_name(err));
        free(blob_data);
        return false;
    }

    size_t restored_count = read_size / sizeof(world_block_delta_t);
    if (restored_count > WORLD_MAX_BLOCK_DELTAS)
    {
        restored_count = WORLD_MAX_BLOCK_DELTAS;
    }

    memcpy(s_world_deltas.entries,
           blob_data,
           restored_count * sizeof(world_block_delta_t));
    s_world_deltas.count = restored_count;
    free(blob_data);

    ESP_LOGW(TAG,
             "restored %u world delta entries from NVS",
             (unsigned int)s_world_deltas.count);
    return true;
}

static bool persist_world_deltas_to_nvs(void)
{
    nvs_handle_t handle = 0;
    esp_err_t err = nvs_open(PROTO_WORLD_NVS_NAMESPACE, NVS_READWRITE, &handle);
    if (err != ESP_OK)
    {
        ESP_LOGW(TAG, "nvs_open(write) failed: %s", esp_err_to_name(err));
        return false;
    }

    if (s_world_deltas.count == 0)
    {
        err = nvs_erase_key(handle, PROTO_WORLD_NVS_KEY);
        if (err == ESP_ERR_NVS_NOT_FOUND)
        {
            err = ESP_OK;
        }
    }
    else
    {
        size_t data_size = s_world_deltas.count * sizeof(world_block_delta_t);
        err = nvs_set_blob(handle,
                           PROTO_WORLD_NVS_KEY,
                           s_world_deltas.entries,
                           data_size);
    }

    if (err == ESP_OK)
    {
        err = nvs_commit(handle);
    }

    nvs_close(handle);

    if (err != ESP_OK)
    {
        ESP_LOGW(TAG, "world delta persist failed: %s", esp_err_to_name(err));
        return false;
    }

    return true;
}

static int32_t query_block_state_id(int32_t x, int32_t y, int32_t z)
{
    uint8_t block_id = query_block_id(x, y, z);
    return world_block_to_state_id(block_id);
}

static bool set_block_override(int32_t x,
                               int32_t y,
                               int32_t z,
                               uint8_t block_id,
                               bool *changed_out)
{
    ensure_world_initialized();
    *changed_out = false;

    if (y < s_world_config.min_y || y > s_world_config.max_y)
    {
        return true;
    }

    int32_t current_state_id = query_block_state_id(x, y, z);
    int32_t next_state_id = world_block_to_state_id(block_id);
    if (current_state_id == next_state_id)
    {
        return true;
    }

    if (!world_deltas_put(&s_world_deltas, x, y, z, block_id))
    {
        return false;
    }

    if (!persist_world_deltas_to_nvs())
    {
        ESP_LOGW(TAG, "continuing with in-memory world deltas only");
    }

    *changed_out = true;
    return true;
}

static bool build_block_change_packet_body(int32_t x,
                                           int32_t y,
                                           int32_t z,
                                           int32_t block_state_id,
                                           size_t *packet_length_out)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x0B) ||
        !write_block_position(&writer, x, y, z) ||
        !proto_write_varint(&writer, block_state_id))
    {
        return false;
    }

    *packet_length_out = writer.length;
    return true;
}

static void publish_block_change(int socket_fd,
                                 int32_t x,
                                 int32_t y,
                                 int32_t z,
                                 int32_t block_state_id,
                                 proto_send_callback_t send_fn,
                                 proto_broadcast_callback_t broadcast_fn,
                                 void *send_context)
{
    size_t packet_length = 0;
    if (!build_block_change_packet_body(x, y, z, block_state_id, &packet_length))
    {
        ESP_LOGW(TAG, "block change packet build failed at (%ld,%ld,%ld)",
                 (long)x,
                 (long)y,
                 (long)z);
        return;
    }

    if (!send_packet(socket_fd,
                     s_proto_packet_buffer,
                     packet_length,
                     send_fn,
                     send_context))
    {
        ESP_LOGW(TAG, "block change send failed for source socket=%d", socket_fd);
    }

    if (!broadcast_packet(socket_fd,
                          s_proto_packet_buffer,
                          packet_length,
                          broadcast_fn,
                          send_context))
    {
        ESP_LOGW(TAG, "block change broadcast had delivery failures");
    }
}

static void fill_height_samples(int32_t chunk_x,
                                int32_t chunk_z,
                                uint16_t height_samples[PROTO_HEIGHTMAP_ENTRY_COUNT])
{
    ensure_world_initialized();

    int32_t max_scan_y = s_world_config.max_y;
    if (max_scan_y > 255)
    {
        max_scan_y = 255;
    }

    for (int32_t z = 0; z < PROTO_SECTION_WIDTH; z++)
    {
        for (int32_t x = 0; x < PROTO_SECTION_WIDTH; x++)
        {
            int32_t world_x = (chunk_x << 4) + x;
            int32_t world_z = (chunk_z << 4) + z;
            uint16_t height = 0;

            for (int32_t y = max_scan_y; y >= s_world_config.min_y; y--)
            {
                if (query_block_state_id(world_x, y, world_z) != 0)
                {
                    height = (uint16_t)(y + 1);
                    break;
                }
            }

            height_samples[(z * PROTO_SECTION_WIDTH) + x] = height;
        }
    }
}

static void pack_heightmap(const uint16_t height_samples[PROTO_HEIGHTMAP_ENTRY_COUNT],
                           uint64_t packed_heightmap[PROTO_HEIGHTMAP_LONG_COUNT])
{
    memset(packed_heightmap, 0, sizeof(uint64_t) * PROTO_HEIGHTMAP_LONG_COUNT);

    for (int32_t index = 0; index < PROTO_HEIGHTMAP_ENTRY_COUNT; index++)
    {
        uint64_t value = (uint64_t)(height_samples[index] & 0x1FFu);
        int32_t bit_index = index * PROTO_HEIGHTMAP_BITS_PER_ENTRY;
        int32_t long_index = bit_index / 64;
        int32_t bit_offset = bit_index % 64;

        packed_heightmap[long_index] |= value << bit_offset;
        if (bit_offset > (64 - PROTO_HEIGHTMAP_BITS_PER_ENTRY) && (long_index + 1) < PROTO_HEIGHTMAP_LONG_COUNT)
        {
            packed_heightmap[long_index + 1] |= value >> (64 - bit_offset);
        }
    }
}

static bool write_chunk_heightmaps_nbt(proto_writer_t *writer,
                                       const uint16_t height_samples[PROTO_HEIGHTMAP_ENTRY_COUNT])
{
    uint64_t packed_unsigned[PROTO_HEIGHTMAP_LONG_COUNT];
    int64_t packed_signed[PROTO_HEIGHTMAP_LONG_COUNT];

    pack_heightmap(height_samples, packed_unsigned);
    for (size_t i = 0; i < PROTO_HEIGHTMAP_LONG_COUNT; i++)
    {
        packed_signed[i] = (int64_t)packed_unsigned[i];
    }

    return nbt_begin_named_compound(writer, "") &&
           nbt_write_named_long_array(writer,
                                      "MOTION_BLOCKING",
                                      packed_signed,
                                      PROTO_HEIGHTMAP_LONG_COUNT) &&
           nbt_write_named_long_array(writer,
                                      "WORLD_SURFACE",
                                      packed_signed,
                                      PROTO_HEIGHTMAP_LONG_COUNT) &&
           nbt_end_compound(writer);
}

static bool encode_chunk_sections(int32_t chunk_x,
                                  int32_t chunk_z,
                                  int32_t *section_mask_out,
                                  size_t *chunk_data_length_out)
{
    proto_writer_t chunk_writer;
    proto_writer_init(&chunk_writer, s_proto_chunk_data_buffer, sizeof(s_proto_chunk_data_buffer));

    int32_t section_mask = 0;
    int32_t max_section_index = s_world_config.max_y / PROTO_SECTION_HEIGHT;
    if (max_section_index < 0)
    {
        max_section_index = 0;
    }
    if (max_section_index >= PROTO_SECTION_COUNT)
    {
        max_section_index = PROTO_SECTION_COUNT - 1;
    }

    for (int32_t section_index = 0; section_index <= max_section_index; section_index++)
    {
        int32_t section_base_y = section_index * PROTO_SECTION_HEIGHT;
        int32_t solid_block_count = 0;
        uint8_t palette_indices[PROTO_SECTION_VOLUME] = {0};

        for (int32_t block_index = 0; block_index < PROTO_SECTION_VOLUME; block_index++)
        {
            int32_t local_x = block_index & 0x0F;
            int32_t local_z = (block_index >> 4) & 0x0F;
            int32_t local_y = (block_index >> 8) & 0x0F;

            int32_t world_x = (chunk_x << 4) + local_x;
            int32_t world_y = section_base_y + local_y;
            int32_t world_z = (chunk_z << 4) + local_z;

            uint8_t palette_index = world_block_to_palette_index(query_block_id(world_x,
                                                                                 world_y,
                                                                                 world_z));
            palette_indices[block_index] = palette_index;
            if (palette_index != 0)
            {
                solid_block_count++;
            }
        }

        if (solid_block_count == 0)
        {
            continue;
        }

        section_mask |= (1 << section_index);

        if (!proto_write_u16_be(&chunk_writer, (uint16_t)solid_block_count) ||
            !proto_write_u8(&chunk_writer, PROTO_CHUNK_BITS_PER_BLOCK) ||
            !proto_write_varint(&chunk_writer, PROTO_CHUNK_PALETTE_SIZE))
        {
            return false;
        }

        for (int32_t palette_index = 0; palette_index < PROTO_CHUNK_PALETTE_SIZE; palette_index++)
        {
            if (!proto_write_varint(&chunk_writer, s_chunk_palette_state_ids[palette_index]))
            {
                return false;
            }
        }

        if (!proto_write_varint(&chunk_writer, PROTO_SECTION_VOLUME / 16))
        {
            return false;
        }

        for (int32_t long_index = 0; long_index < (PROTO_SECTION_VOLUME / 16); long_index++)
        {
            uint64_t packed = 0;
            for (int32_t value_index = 0; value_index < 16; value_index++)
            {
                int32_t block_index = (long_index * 16) + value_index;
                uint64_t palette_index = (uint64_t)palette_indices[block_index];
                packed |= palette_index << (value_index * 4);
            }

            if (!proto_write_i64_be(&chunk_writer, (int64_t)packed))
            {
                return false;
            }
        }
    }

    *section_mask_out = section_mask;
    *chunk_data_length_out = chunk_writer.length;
    return true;
}

static bool send_update_view_position_packet(int socket_fd,
                                             int32_t chunk_x,
                                             int32_t chunk_z,
                                             proto_send_callback_t send_fn,
                                             void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x40) ||
        !proto_write_varint(&writer, chunk_x) ||
        !proto_write_varint(&writer, chunk_z))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_unload_chunk_packet(int socket_fd,
                                     int32_t chunk_x,
                                     int32_t chunk_z,
                                     proto_send_callback_t send_fn,
                                     void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x1C) ||
        !write_i32_be(&writer, chunk_x) ||
        !write_i32_be(&writer, chunk_z))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_map_chunk_packet(int socket_fd,
                                  int32_t chunk_x,
                                  int32_t chunk_z,
                                  proto_send_callback_t send_fn,
                                  void *send_context)
{
    uint16_t height_samples[PROTO_HEIGHTMAP_ENTRY_COUNT];
    fill_height_samples(chunk_x, chunk_z, height_samples);

    int32_t section_mask = 0;
    size_t chunk_data_length = 0;
    if (!encode_chunk_sections(chunk_x, chunk_z, &section_mask, &chunk_data_length))
    {
        return false;
    }

    if (section_mask < 0 ||
        chunk_data_length > sizeof(s_proto_chunk_data_buffer) ||
        chunk_data_length > (size_t)INT32_MAX)
    {
        return false;
    }

    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x20) ||
        !write_i32_be(&writer, chunk_x) ||
        !write_i32_be(&writer, chunk_z) ||
        !proto_write_u8(&writer, 1) ||
        !proto_write_varint(&writer, section_mask) ||
        !write_chunk_heightmaps_nbt(&writer, height_samples) ||
        !proto_write_varint(&writer, PROTO_BIOME_ARRAY_COUNT))
    {
        return false;
    }

    for (int32_t i = 0; i < PROTO_BIOME_ARRAY_COUNT; i++)
    {
        if (!proto_write_varint(&writer, 1))
        {
            return false;
        }
    }

    if (!proto_write_varint(&writer, (int32_t)chunk_data_length) ||
        !proto_write_bytes(&writer, s_proto_chunk_data_buffer, chunk_data_length) ||
        !proto_write_varint(&writer, 0))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_update_light_packet(int socket_fd,
                                     int32_t chunk_x,
                                     int32_t chunk_z,
                                     proto_send_callback_t send_fn,
                                     void *send_context)
{
    if (PROTO_CHUNK_LIGHT_EMPTY_MASK <= 0)
    {
        return false;
    }

    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x23) ||
        !proto_write_varint(&writer, chunk_x) ||
        !proto_write_varint(&writer, chunk_z) ||
        !proto_write_u8(&writer, 1) ||
        !proto_write_varint(&writer, 0) ||
        !proto_write_varint(&writer, 0) ||
        !proto_write_varint(&writer, PROTO_CHUNK_LIGHT_EMPTY_MASK) ||
        !proto_write_varint(&writer, PROTO_CHUNK_LIGHT_EMPTY_MASK) ||
        !proto_write_varint(&writer, 0) ||
        !proto_write_varint(&writer, 0))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool write_mood_sound_compound(proto_writer_t *writer)
{
    return nbt_write_named_double(writer, "offset", 2.0) &&
           nbt_write_named_string(writer, "sound", "minecraft:ambient.cave") &&
           nbt_write_named_int(writer, "block_search_extent", 8) &&
           nbt_write_named_int(writer, "tick_delay", 6000) &&
           nbt_end_compound(writer);
}

static bool write_biome_effects_compound(proto_writer_t *writer)
{
    return nbt_write_named_int(writer, "water_fog_color", 329011) &&
           nbt_write_named_int(writer, "water_color", 4159204) &&
           nbt_write_named_int(writer, "fog_color", 12638463) &&
           nbt_begin_named_compound(writer, "mood_sound") &&
           write_mood_sound_compound(writer) &&
           nbt_write_named_int(writer, "sky_color", 7907327) &&
           nbt_end_compound(writer);
}

static bool write_plains_biome_element_compound(proto_writer_t *writer)
{
    return nbt_write_named_string(writer, "category", "plains") &&
           nbt_write_named_float(writer, "temperature", 0.8f) &&
           nbt_write_named_float(writer, "downfall", 0.4f) &&
           nbt_write_named_float(writer, "depth", 0.125f) &&
           nbt_begin_named_compound(writer, "effects") &&
           write_biome_effects_compound(writer) &&
           nbt_write_named_string(writer, "precipitation", "rain") &&
           nbt_write_named_float(writer, "scale", 0.05f) &&
           nbt_end_compound(writer);
}

static bool write_plains_biome_registry_entry(proto_writer_t *writer)
{
    return nbt_write_named_string(writer, "name", "minecraft:plains") &&
           nbt_write_named_int(writer, "id", 1) &&
           nbt_begin_named_compound(writer, "element") &&
           write_plains_biome_element_compound(writer) &&
           nbt_end_compound(writer);
}

static bool write_overworld_dimension_fields(proto_writer_t *writer)
{
    return nbt_write_named_byte(writer, "bed_works", 1) &&
           nbt_write_named_byte(writer, "has_ceiling", 0) &&
           nbt_write_named_double(writer, "coordinate_scale", 1.0) &&
           nbt_write_named_byte(writer, "piglin_safe", 0) &&
           nbt_write_named_byte(writer, "has_skylight", 1) &&
           nbt_write_named_byte(writer, "ultrawarm", 0) &&
           nbt_write_named_string(writer, "infiniburn", "minecraft:infiniburn_overworld") &&
           nbt_write_named_string(writer, "effects", "minecraft:overworld") &&
           nbt_write_named_byte(writer, "has_raids", 1) &&
           nbt_write_named_float(writer, "ambient_light", 0.0f) &&
           nbt_write_named_int(writer, "logical_height", 256) &&
           nbt_write_named_byte(writer, "natural", 1) &&
           nbt_write_named_byte(writer, "respawn_anchor_works", 0);
}

static bool write_overworld_dimension_registry_entry(proto_writer_t *writer)
{
    return nbt_write_named_string(writer, "name", "minecraft:overworld") &&
           nbt_write_named_int(writer, "id", 0) &&
           nbt_begin_named_compound(writer, "element") &&
           write_overworld_dimension_fields(writer) &&
           nbt_end_compound(writer) &&
           nbt_end_compound(writer);
}

static bool write_dimension_codec_nbt(proto_writer_t *writer)
{
    return nbt_begin_named_compound(writer, "") &&
           nbt_begin_named_compound(writer, "minecraft:dimension_type") &&
           nbt_write_named_string(writer, "type", "minecraft:dimension_type") &&
           nbt_begin_named_list(writer, "value", NBT_TAG_COMPOUND, 1) &&
           write_overworld_dimension_registry_entry(writer) &&
           nbt_end_compound(writer) &&
           nbt_begin_named_compound(writer, "minecraft:worldgen/biome") &&
           nbt_write_named_string(writer, "type", "minecraft:worldgen/biome") &&
           nbt_begin_named_list(writer, "value", NBT_TAG_COMPOUND, 1) &&
           write_plains_biome_registry_entry(writer) &&
           nbt_end_compound(writer) &&
           nbt_end_compound(writer);
}

static bool write_dimension_nbt(proto_writer_t *writer)
{
    return nbt_begin_named_compound(writer, "") &&
           write_overworld_dimension_fields(writer) &&
           nbt_end_compound(writer);
}

static bool send_packet(int socket_fd,
                        const uint8_t *packet_body,
                        size_t packet_length,
                        proto_send_callback_t send_fn,
                        void *send_context)
{
    size_t framed_length = 0;

    if (send_fn == NULL || (packet_length > 0 && packet_body == NULL))
    {
        return false;
    }

    if (packet_length > SERVER_MAX_OUTBOUND_PACKET_SIZE)
    {
        return false;
    }

    if (!proto_wrap_packet(packet_body,
                           packet_length,
                           s_proto_framed_buffer,
                           sizeof(s_proto_framed_buffer),
                           &framed_length))
    {
        return false;
    }

    if (framed_length == 0 || framed_length > sizeof(s_proto_framed_buffer))
    {
        return false;
    }

    return send_fn(send_context, socket_fd, s_proto_framed_buffer, framed_length);
}

static bool broadcast_packet(int source_socket_fd,
                             const uint8_t *packet_body,
                             size_t packet_length,
                             proto_broadcast_callback_t broadcast_fn,
                             void *send_context)
{
    if (broadcast_fn == NULL)
    {
        return true;
    }

    size_t framed_length = 0;

    if (packet_length > SERVER_MAX_OUTBOUND_PACKET_SIZE)
    {
        return false;
    }

    if (!proto_wrap_packet(packet_body,
                           packet_length,
                           s_proto_framed_buffer,
                           sizeof(s_proto_framed_buffer),
                           &framed_length))
    {
        return false;
    }

    return broadcast_fn(send_context, source_socket_fd, s_proto_framed_buffer, framed_length);
}

static bool send_status_response(int socket_fd,
                                 const proto_server_info_t *server,
                                 proto_send_callback_t send_fn,
                                 void *send_context)
{
    char escaped_motd[128];
    if (!escape_json_text(server->motd, escaped_motd, sizeof(escaped_motd)))
    {
        return false;
    }

    char json[320];
    int written = snprintf(json,
                           sizeof(json),
                           "{\"version\":{\"name\":\"1.16.5\",\"protocol\":%u},"
                           "\"players\":{\"max\":%u,\"online\":%u},"
                           "\"description\":{\"text\":\"%s\"}}",
                           (unsigned int)server->protocol_version,
                           (unsigned int)server->max_players,
                           (unsigned int)server->online_players,
                           escaped_motd);
    if (written <= 0 || written >= (int)sizeof(json))
    {
        return false;
    }

    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x00))
    {
        return false;
    }
    if (!proto_write_string(&writer, json))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_pong(int socket_fd,
                      int64_t payload,
                      proto_send_callback_t send_fn,
                      void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x01))
    {
        return false;
    }
    if (!proto_write_i64_be(&writer, payload))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_login_disconnect(int socket_fd,
                                  const char *message,
                                  proto_send_callback_t send_fn,
                                  void *send_context)
{
    char escaped[160];
    if (!escape_json_text(message, escaped, sizeof(escaped)))
    {
        return false;
    }

    char json[192];
    int written = snprintf(json, sizeof(json), "{\"text\":\"%s\"}", escaped);
    if (written <= 0 || written >= (int)sizeof(json))
    {
        return false;
    }

    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x00))
    {
        return false;
    }
    if (!proto_write_string(&writer, json))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_login_success(int socket_fd,
                               const proto_connection_t *connection,
                               proto_send_callback_t send_fn,
                               void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x02) ||
        !proto_write_i64_be(&writer, connection->uuid_most) ||
        !proto_write_i64_be(&writer, connection->uuid_least) ||
        !proto_write_string(&writer, connection->username))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_play_login(int socket_fd,
                            const proto_connection_t *connection,
                            const proto_server_info_t *server,
                            proto_send_callback_t send_fn,
                            void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x24) ||
        !write_i32_be(&writer, connection->entity_id) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_u8(&writer, 0xFF) ||
        !proto_write_varint(&writer, 1) ||
        !proto_write_string(&writer, "minecraft:overworld") ||
        !write_dimension_codec_nbt(&writer) ||
        !write_dimension_nbt(&writer) ||
        !proto_write_string(&writer, "minecraft:overworld") ||
        !proto_write_i64_be(&writer, 0) ||
        !proto_write_varint(&writer, server->max_players) ||
        !proto_write_varint(&writer, SERVER_VIEW_DISTANCE_CHUNKS) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_u8(&writer, 1) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_u8(&writer, 0))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_initial_position(int socket_fd,
                                  const proto_connection_t *connection,
                                  proto_send_callback_t send_fn,
                                  void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x34) ||
        !write_f64_be(&writer, connection->pos_x) ||
        !write_f64_be(&writer, connection->pos_y) ||
        !write_f64_be(&writer, connection->pos_z) ||
        !write_f32_be(&writer, connection->yaw) ||
        !write_f32_be(&writer, connection->pitch) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_varint(&writer, 0))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_keepalive(int socket_fd,
                           int64_t keepalive_id,
                           proto_send_callback_t send_fn,
                           void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x1F) ||
        !proto_write_i64_be(&writer, keepalive_id))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool send_update_health_packet(int socket_fd,
                                      const proto_connection_t *connection,
                                      proto_send_callback_t send_fn,
                                      void *send_context)
{
    proto_writer_t writer;
    proto_writer_init(&writer, s_proto_packet_buffer, sizeof(s_proto_packet_buffer));

    if (!proto_write_varint(&writer, 0x49) ||
        !write_f32_be(&writer, connection->health) ||
        !proto_write_varint(&writer, connection->food_level) ||
        !write_f32_be(&writer, connection->food_saturation))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, writer.length, send_fn, send_context);
}

static bool build_chat_packet_body(const char *message_text,
                                   int64_t uuid_most,
                                   int64_t uuid_least,
                                   uint8_t *packet,
                                   size_t packet_capacity,
                                   size_t *packet_length)
{
    char escaped_message[512];
    if (!escape_json_text(message_text, escaped_message, sizeof(escaped_message)))
    {
        return false;
    }

    char json[560];
    int written = snprintf(json, sizeof(json), "{\"text\":\"%s\"}", escaped_message);
    if (written <= 0 || written >= (int)sizeof(json))
    {
        return false;
    }

    proto_writer_t writer;
    proto_writer_init(&writer, packet, packet_capacity);

    if (!proto_write_varint(&writer, 0x0E) ||
        !proto_write_string(&writer, json) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_i64_be(&writer, uuid_most) ||
        !proto_write_i64_be(&writer, uuid_least))
    {
        return false;
    }

    *packet_length = writer.length;
    return true;
}

bool proto_build_chat_packet(const char *message_text,
                             int64_t uuid_most,
                             int64_t uuid_least,
                             uint8_t *framed_packet_out,
                             size_t framed_packet_capacity,
                             size_t *framed_packet_length)
{
    uint8_t packet_body[SERVER_MAX_INBOUND_PACKET_SIZE];
    size_t packet_body_length = 0;

    if (!build_chat_packet_body(message_text,
                                uuid_most,
                                uuid_least,
                                packet_body,
                                sizeof(packet_body),
                                &packet_body_length))
    {
        return false;
    }

    return proto_wrap_packet(packet_body,
                             packet_body_length,
                             framed_packet_out,
                             framed_packet_capacity,
                             framed_packet_length);
}

static bool send_chat_text_to_client(int socket_fd,
                                     const char *message_text,
                                     int64_t uuid_most,
                                     int64_t uuid_least,
                                     proto_send_callback_t send_fn,
                                     void *send_context)
{
    size_t packet_length = 0;

    if (!build_chat_packet_body(message_text,
                                uuid_most,
                                uuid_least,
                                s_proto_packet_buffer,
                                sizeof(s_proto_packet_buffer),
                                &packet_length))
    {
        return false;
    }

    return send_packet(socket_fd, s_proto_packet_buffer, packet_length, send_fn, send_context);
}

static bool broadcast_chat_text(int source_socket_fd,
                                const char *message_text,
                                int64_t uuid_most,
                                int64_t uuid_least,
                                proto_broadcast_callback_t broadcast_fn,
                                void *send_context)
{
    size_t packet_length = 0;

    if (!build_chat_packet_body(message_text,
                                uuid_most,
                                uuid_least,
                                s_proto_packet_buffer,
                                sizeof(s_proto_packet_buffer),
                                &packet_length))
    {
        return false;
    }

    return broadcast_packet(source_socket_fd,
                            s_proto_packet_buffer,
                            packet_length,
                            broadcast_fn,
                            send_context);
}

static void reset_chunk_stream_state(proto_connection_t *connection)
{
    connection->chunk_stream_initialized = false;
    connection->chunk_center_x = 0;
    connection->chunk_center_z = 0;
    connection->has_previous_chunk_center = false;
    connection->previous_chunk_center_x = 0;
    connection->previous_chunk_center_z = 0;
    connection->chunk_unload_grace_until_ms = 0;
    connection->chunk_scan_index = 0;
    connection->chunk_sent_count = 0;
}

static void initialize_survival_state(proto_connection_t *connection, uint64_t now_ms)
{
    connection->health = 20.0f;
    connection->food_level = 20;
    connection->food_saturation = 5.0f;
    connection->next_hunger_decay_ms = now_ms + PROTO_HUNGER_DECAY_INTERVAL_MS;
    connection->next_health_regen_ms = now_ms + PROTO_HEALTH_REGEN_INTERVAL_MS;
    connection->next_starvation_damage_ms = now_ms + PROTO_STARVATION_DAMAGE_INTERVAL_MS;
    connection->next_void_damage_ms = now_ms + PROTO_VOID_DAMAGE_INTERVAL_MS;
}

static bool is_chunk_tracked(const proto_connection_t *connection, int32_t chunk_x, int32_t chunk_z)
{
    for (uint16_t i = 0; i < connection->chunk_sent_count; i++)
    {
        if (connection->sent_chunks[i].x == chunk_x &&
            connection->sent_chunks[i].z == chunk_z)
        {
            return true;
        }
    }

    return false;
}

static void track_chunk(proto_connection_t *connection, int32_t chunk_x, int32_t chunk_z)
{
    if (connection->chunk_sent_count >= SERVER_CHUNK_TRACKED_MAX)
    {
        connection->chunk_sent_count = 0;
    }

    connection->sent_chunks[connection->chunk_sent_count].x = chunk_x;
    connection->sent_chunks[connection->chunk_sent_count].z = chunk_z;
    connection->chunk_sent_count++;
}

static bool is_chunk_in_window(int32_t center_x,
                               int32_t center_z,
                               int32_t chunk_x,
                               int32_t chunk_z)
{
    int32_t min_x = center_x - SERVER_CHUNK_SEND_RADIUS;
    int32_t max_x = center_x + SERVER_CHUNK_SEND_RADIUS;
    int32_t min_z = center_z - SERVER_CHUNK_SEND_RADIUS;
    int32_t max_z = center_z + SERVER_CHUNK_SEND_RADIUS;

    return chunk_x >= min_x && chunk_x <= max_x &&
           chunk_z >= min_z && chunk_z <= max_z;
}

static void prune_tracked_chunks(proto_connection_t *connection,
                                 int socket_fd,
                                 proto_send_callback_t send_fn,
                                 void *send_context,
                                 uint64_t now_ms)
{
    bool keep_previous_center_window = connection->has_previous_chunk_center &&
                                       now_ms < connection->chunk_unload_grace_until_ms;

    uint16_t index = 0;
    while (index < connection->chunk_sent_count)
    {
        int32_t chunk_x = connection->sent_chunks[index].x;
        int32_t chunk_z = connection->sent_chunks[index].z;

        if (is_chunk_in_window(connection->chunk_center_x,
                               connection->chunk_center_z,
                               chunk_x,
                               chunk_z) ||
            (keep_previous_center_window &&
             is_chunk_in_window(connection->previous_chunk_center_x,
                                connection->previous_chunk_center_z,
                                chunk_x,
                                chunk_z)))
        {
            index++;
            continue;
        }

        if (!send_unload_chunk_packet(socket_fd, chunk_x, chunk_z, send_fn, send_context))
        {
            ESP_LOGW(TAG,
                     "unload chunk send failed for (%ld,%ld)",
                     (long)chunk_x,
                     (long)chunk_z);
            index++;
            continue;
        }

        uint16_t last_index = connection->chunk_sent_count - 1;
        if (index != last_index)
        {
            connection->sent_chunks[index] = connection->sent_chunks[last_index];
        }
        connection->chunk_sent_count--;
    }
}

static int32_t chunk_coord_from_position(double position)
{
    return (int32_t)floor(position / 16.0);
}

static bool send_next_chunk_for_connection(proto_connection_t *connection,
                                           int socket_fd,
                                           proto_send_callback_t send_fn,
                                           void *send_context)
{
    int32_t diameter = (SERVER_CHUNK_SEND_RADIUS * 2) + 1;
    int32_t total = diameter * diameter;

    for (int32_t attempt = 0; attempt < total; attempt++)
    {
        int32_t scan_index = (connection->chunk_scan_index + (uint16_t)attempt) % (uint16_t)total;
        int32_t local_dx = (scan_index % diameter) - SERVER_CHUNK_SEND_RADIUS;
        int32_t local_dz = (scan_index / diameter) - SERVER_CHUNK_SEND_RADIUS;
        int32_t chunk_x = connection->chunk_center_x + local_dx;
        int32_t chunk_z = connection->chunk_center_z + local_dz;

        if (is_chunk_tracked(connection, chunk_x, chunk_z))
        {
            continue;
        }

        if (!send_map_chunk_packet(socket_fd, chunk_x, chunk_z, send_fn, send_context) ||
            !send_update_light_packet(socket_fd, chunk_x, chunk_z, send_fn, send_context))
        {
            ESP_LOGW(TAG,
                     "chunk send failed at (%ld,%ld); will retry",
                     (long)chunk_x,
                     (long)chunk_z);
            connection->chunk_scan_index = (uint16_t)((scan_index + 1) % total);
            return true;
        }

        track_chunk(connection, chunk_x, chunk_z);
        connection->chunk_scan_index = (uint16_t)((scan_index + 1) % total);
        return true;
    }

    return true;
}

void proto_connection_reset(proto_connection_t *connection)
{
    memset(connection, 0, sizeof(*connection));
    connection->state = PROTO_STATE_HANDSHAKE;
    connection->pos_x = 0.0;
    connection->pos_y = 80.0;
    connection->pos_z = 0.0;
    connection->yaw = 0.0f;
    connection->pitch = 0.0f;
    connection->on_ground = true;
    connection->health = 20.0f;
    connection->food_level = 20;
    connection->food_saturation = 5.0f;
    reset_chunk_stream_state(connection);
}

static void handle_handshake(proto_connection_t *connection, proto_reader_t *reader)
{
    int32_t protocol_version = 0;
    int32_t next_state = 0;
    char server_address[256];
    uint16_t server_port = 0;

    if (!proto_read_varint(reader, &protocol_version) ||
        !proto_read_string(reader, server_address, sizeof(server_address)) ||
        !proto_read_u16_be(reader, &server_port) ||
        !proto_read_varint(reader, &next_state))
    {
        ESP_LOGW(TAG, "handshake parse failed");
        connection->close_requested = true;
        return;
    }

    (void)protocol_version;
    (void)server_address;
    (void)server_port;

    if (next_state == 1)
    {
        connection->state = PROTO_STATE_STATUS;
    }
    else if (next_state == 2)
    {
        connection->state = PROTO_STATE_LOGIN;
    }
    else
    {
        ESP_LOGW(TAG, "invalid handshake next_state=%ld", (long)next_state);
        connection->close_requested = true;
    }
}

static void handle_status(proto_connection_t *connection,
                          int32_t packet_id,
                          proto_reader_t *reader,
                          int socket_fd,
                          const proto_server_info_t *server,
                          proto_send_callback_t send_fn,
                          void *send_context)
{
    if (packet_id == 0x00)
    {
        if (!send_status_response(socket_fd, server, send_fn, send_context))
        {
            ESP_LOGW(TAG, "status response send failed");
            connection->close_requested = true;
        }
        return;
    }

    if (packet_id == 0x01)
    {
        int64_t payload = 0;
        if (!proto_read_i64_be(reader, &payload))
        {
            ESP_LOGW(TAG, "status ping parse failed");
            connection->close_requested = true;
            return;
        }

        if (!send_pong(socket_fd, payload, send_fn, send_context))
        {
            ESP_LOGW(TAG, "status pong send failed");
            connection->close_requested = true;
            return;
        }

        connection->close_requested = true;
        return;
    }

    ESP_LOGW(TAG, "unexpected status packet id=0x%lx", (long)packet_id);
    connection->close_requested = true;
}

static void handle_login(proto_connection_t *connection,
                         int32_t packet_id,
                         proto_reader_t *reader,
                         int socket_fd,
                         const proto_server_info_t *server,
                         proto_send_callback_t send_fn,
                         proto_broadcast_callback_t broadcast_fn,
                         void *send_context,
                         uint64_t now_ms)
{
    if (packet_id != 0x00)
    {
        ESP_LOGW(TAG, "unexpected login packet id=0x%lx", (long)packet_id);
        connection->close_requested = true;
        return;
    }

    if (!proto_read_string(reader, connection->username, sizeof(connection->username)))
    {
        ESP_LOGW(TAG, "login username parse failed");
        connection->close_requested = true;
        return;
    }

    if (connection->username[0] == '\0')
    {
        ESP_LOGW(TAG, "login rejected: empty username");
        send_login_disconnect(socket_fd, "Username is required.", send_fn, send_context);
        connection->close_requested = true;
        return;
    }

    connection->entity_id = next_entity_id();
    generate_offline_uuid(connection->username, &connection->uuid_most, &connection->uuid_least);

    if (!send_login_success(socket_fd, connection, send_fn, send_context))
    {
        ESP_LOGW(TAG, "login success packet send failed: user=%s", connection->username);
        connection->close_requested = true;
        return;
    }

    connection->state = PROTO_STATE_PLAY;
    connection->joined_play = true;
    connection->awaiting_keepalive = false;
    connection->next_keepalive_ms = now_ms + PROTO_KEEPALIVE_INTERVAL_MS;
    connection->last_activity_ms = now_ms;
    initialize_survival_state(connection, now_ms);

    if (!send_play_login(socket_fd, connection, server, send_fn, send_context) ||
        !send_initial_position(socket_fd, connection, send_fn, send_context))
    {
        ESP_LOGW(TAG, "play init packet send failed: user=%s", connection->username);
        connection->close_requested = true;
        return;
    }

    reset_chunk_stream_state(connection);
    connection->chunk_center_x = chunk_coord_from_position(connection->pos_x);
    connection->chunk_center_z = chunk_coord_from_position(connection->pos_z);
    connection->chunk_stream_initialized = true;

    if (!send_update_view_position_packet(socket_fd,
                                          connection->chunk_center_x,
                                          connection->chunk_center_z,
                                          send_fn,
                                          send_context))
    {
        ESP_LOGW(TAG, "initial view position send failed: user=%s", connection->username);
        connection->close_requested = true;
        return;
    }

    if (!send_chat_text_to_client(socket_fd,
                                  "ESP32 server online. Movement and chat are active.",
                                  0,
                                  0,
                                  send_fn,
                                  send_context))
    {
        ESP_LOGW(TAG, "welcome chat send failed: user=%s", connection->username);
        connection->close_requested = true;
        return;
    }

    if (!send_update_health_packet(socket_fd, connection, send_fn, send_context))
    {
        ESP_LOGW(TAG, "initial health send failed: user=%s", connection->username);
        connection->close_requested = true;
        return;
    }

    ESP_LOGW(TAG,
             "player session started: user=%s entity_id=%ld",
             connection->username,
             (long)connection->entity_id);

    char joined_message[96];
    int joined_written = snprintf(joined_message,
                                  sizeof(joined_message),
                                  "%s joined the game",
                                  connection->username);
    if (joined_written > 0 && joined_written < (int)sizeof(joined_message))
    {
        broadcast_chat_text(socket_fd, joined_message, 0, 0, broadcast_fn, send_context);
    }
}

static void handle_play_keepalive(proto_connection_t *connection,
                                  proto_reader_t *reader,
                                  uint64_t now_ms)
{
    int64_t keepalive_id = 0;
    if (!proto_read_i64_be(reader, &keepalive_id))
    {
        ESP_LOGW(TAG, "keepalive parse failed: user=%s",
                 connection->username[0] != '\0' ? connection->username : "(unknown)");
        connection->close_requested = true;
        return;
    }

    if (!connection->awaiting_keepalive || keepalive_id != connection->last_keepalive_id)
    {
        ESP_LOGW(TAG,
                 "keepalive mismatch: user=%s expected=%lld got=%lld",
                 connection->username[0] != '\0' ? connection->username : "(unknown)",
                 (long long)connection->last_keepalive_id,
                 (long long)keepalive_id);
        connection->close_requested = true;
        return;
    }

    connection->awaiting_keepalive = false;
    connection->keepalive_deadline_ms = 0;
    connection->next_keepalive_ms = now_ms + PROTO_KEEPALIVE_INTERVAL_MS;
}

static bool read_chat_message_limited(proto_reader_t *reader,
                                      char *out,
                                      size_t out_size,
                                      bool *was_clipped)
{
    int32_t encoded_length = 0;
    if (!proto_read_varint(reader, &encoded_length) || encoded_length < 0)
    {
        return false;
    }

    size_t incoming_length = (size_t)encoded_length;
    if (incoming_length > (reader->length - reader->offset))
    {
        return false;
    }

    size_t copy_length = incoming_length;
    *was_clipped = false;

    if (copy_length + 1 > out_size)
    {
        copy_length = out_size - 1;
        *was_clipped = true;
    }

    memcpy(out, reader->data + reader->offset, copy_length);
    out[copy_length] = '\0';
    reader->offset += incoming_length;
    return true;
}

static void handle_play_chat(proto_connection_t *connection,
                             proto_reader_t *reader,
                             int socket_fd,
                             proto_send_callback_t send_fn,
                             proto_broadcast_callback_t broadcast_fn,
                             void *send_context)
{
    char chat_message[SERVER_MAX_CHAT_MESSAGE_LENGTH + 1];
    bool was_clipped = false;

    if (!read_chat_message_limited(reader, chat_message, sizeof(chat_message), &was_clipped))
    {
        ESP_LOGW(TAG,
                 "chat parse failed: user=%s",
                 connection->username[0] != '\0' ? connection->username : "(unknown)");
        connection->close_requested = true;
        return;
    }

    if (was_clipped)
    {
        ESP_LOGW(TAG,
                 "chat clipped to %u chars: user=%s",
                 (unsigned int)SERVER_MAX_CHAT_MESSAGE_LENGTH,
                 connection->username[0] != '\0' ? connection->username : "(unknown)");
        if (!send_chat_text_to_client(socket_fd,
                                      "Message exceeded limit and was clipped.",
                                      0,
                                      0,
                                      send_fn,
                                      send_context))
        {
            connection->close_requested = true;
            return;
        }
    }

    if (chat_message[0] == '\0')
    {
        return;
    }

    if (chat_message[0] == '/')
    {
        ESP_LOGW(TAG,
                 "command rejected: user=%s cmd=%s",
                 connection->username[0] != '\0' ? connection->username : "(unknown)",
                 chat_message);
        if (!send_chat_text_to_client(socket_fd,
                                      "Commands are not supported on this MVP server.",
                                      0,
                                      0,
                                      send_fn,
                                      send_context))
        {
            connection->close_requested = true;
        }
        return;
    }

    char line[sizeof(connection->username) + SERVER_MAX_CHAT_MESSAGE_LENGTH + 8];
    int written = snprintf(line, sizeof(line), "<%s> %s", connection->username, chat_message);
    if (written <= 0 || written >= (int)sizeof(line))
    {
        return;
    }

    ESP_LOGW(TAG, "chat: %s", line);

    if (!send_chat_text_to_client(socket_fd,
                                  line,
                                  connection->uuid_most,
                                  connection->uuid_least,
                                  send_fn,
                                  send_context))
    {
        connection->close_requested = true;
        return;
    }

    broadcast_chat_text(socket_fd,
                        line,
                        connection->uuid_most,
                        connection->uuid_least,
                        broadcast_fn,
                        send_context);
}

static bool is_finite_coordinate(double value)
{
    return isfinite(value) != 0;
}

static void clamp_player_vertical_position(proto_connection_t *connection)
{
    ensure_world_initialized();

    double max_y = (double)s_world_config.max_y + PROTO_MAX_Y_CLAMP_MARGIN;
    if (connection->pos_y > max_y)
    {
        connection->pos_y = max_y;
    }
}

static void handle_play_movement(proto_connection_t *connection,
                                 int32_t packet_id,
                                 proto_reader_t *reader)
{
    switch (packet_id)
    {
    case 0x12:
        if (!read_f64_be(reader, &connection->pos_x) ||
            !read_f64_be(reader, &connection->pos_y) ||
            !read_f64_be(reader, &connection->pos_z) ||
            !read_bool(reader, &connection->on_ground))
        {
            connection->close_requested = true;
        }
        break;

    case 0x13:
        if (!read_f64_be(reader, &connection->pos_x) ||
            !read_f64_be(reader, &connection->pos_y) ||
            !read_f64_be(reader, &connection->pos_z) ||
            !read_f32_be(reader, &connection->yaw) ||
            !read_f32_be(reader, &connection->pitch) ||
            !read_bool(reader, &connection->on_ground))
        {
            connection->close_requested = true;
        }
        break;

    case 0x14:
        if (!read_f32_be(reader, &connection->yaw) ||
            !read_f32_be(reader, &connection->pitch) ||
            !read_bool(reader, &connection->on_ground))
        {
            connection->close_requested = true;
        }
        break;

    case 0x15:
        if (!read_bool(reader, &connection->on_ground))
        {
            connection->close_requested = true;
        }
        break;

    default:
        break;
    }

    if (connection->close_requested)
    {
        return;
    }

    if (!is_finite_coordinate(connection->pos_x) ||
        !is_finite_coordinate(connection->pos_y) ||
        !is_finite_coordinate(connection->pos_z))
    {
        connection->close_requested = true;
        return;
    }

    clamp_player_vertical_position(connection);
}

static bool adjust_position_for_face(int32_t *x, int32_t *y, int32_t *z, int32_t face)
{
    switch (face)
    {
    case 0:
        (*y)--;
        return true;
    case 1:
        (*y)++;
        return true;
    case 2:
        (*z)--;
        return true;
    case 3:
        (*z)++;
        return true;
    case 4:
        (*x)--;
        return true;
    case 5:
        (*x)++;
        return true;
    default:
        return false;
    }
}

static void handle_play_block_dig(proto_connection_t *connection,
                                  proto_reader_t *reader,
                                  int socket_fd,
                                  proto_send_callback_t send_fn,
                                  proto_broadcast_callback_t broadcast_fn,
                                  void *send_context)
{
    int32_t status = 0;
    int32_t world_x = 0;
    int32_t world_y = 0;
    int32_t world_z = 0;
    uint8_t face = 0;

    if (!proto_read_varint(reader, &status) ||
        !read_block_position(reader, &world_x, &world_y, &world_z) ||
        !proto_read_u8(reader, &face))
    {
        ESP_LOGW(TAG, "block dig parse failed");
        connection->close_requested = true;
        return;
    }

    (void)face;

    if (status != 2)
    {
        return;
    }

    bool changed = false;
    if (!set_block_override(world_x, world_y, world_z, BLOCK_AIR, &changed))
    {
        ESP_LOGW(TAG,
                 "block dig rejected at (%ld,%ld,%ld)",
                 (long)world_x,
                 (long)world_y,
                 (long)world_z);
        return;
    }

    if (!changed)
    {
        return;
    }

    publish_block_change(socket_fd,
                         world_x,
                         world_y,
                         world_z,
                         world_block_to_state_id(BLOCK_AIR),
                         send_fn,
                         broadcast_fn,
                         send_context);
}

static void handle_play_block_place(proto_connection_t *connection,
                                    proto_reader_t *reader,
                                    int socket_fd,
                                    proto_send_callback_t send_fn,
                                    proto_broadcast_callback_t broadcast_fn,
                                    void *send_context)
{
    int32_t hand = 0;
    int32_t face = 0;
    int32_t target_x = 0;
    int32_t target_y = 0;
    int32_t target_z = 0;
    float cursor_x = 0.0f;
    float cursor_y = 0.0f;
    float cursor_z = 0.0f;
    bool inside_block = false;

    if (!proto_read_varint(reader, &hand) ||
        !read_block_position(reader, &target_x, &target_y, &target_z) ||
        !proto_read_varint(reader, &face) ||
        !read_f32_be(reader, &cursor_x) ||
        !read_f32_be(reader, &cursor_y) ||
        !read_f32_be(reader, &cursor_z) ||
        !read_bool(reader, &inside_block))
    {
        ESP_LOGW(TAG, "block place parse failed");
        connection->close_requested = true;
        return;
    }

    (void)hand;
    (void)cursor_x;
    (void)cursor_y;
    (void)cursor_z;
    (void)inside_block;

    int32_t place_x = target_x;
    int32_t place_y = target_y;
    int32_t place_z = target_z;
    if (!adjust_position_for_face(&place_x, &place_y, &place_z, face))
    {
        return;
    }

    bool changed = false;
    if (!set_block_override(place_x,
                            place_y,
                            place_z,
                            BLOCK_COBBLESTONE,
                            &changed))
    {
        ESP_LOGW(TAG,
                 "block place rejected at (%ld,%ld,%ld)",
                 (long)place_x,
                 (long)place_y,
                 (long)place_z);
        return;
    }

    if (!changed)
    {
        return;
    }

    publish_block_change(socket_fd,
                         place_x,
                         place_y,
                         place_z,
                         world_block_to_state_id(BLOCK_COBBLESTONE),
                         send_fn,
                         broadcast_fn,
                         send_context);
}

static void handle_play(proto_connection_t *connection,
                        int32_t packet_id,
                        proto_reader_t *reader,
                        int socket_fd,
                        proto_send_callback_t send_fn,
                        proto_broadcast_callback_t broadcast_fn,
                        void *send_context,
                        uint64_t now_ms)
{
    connection->last_activity_ms = now_ms;

    switch (packet_id)
    {
    case 0x10:
        handle_play_keepalive(connection, reader, now_ms);
        break;

    case 0x03:
        handle_play_chat(connection, reader, socket_fd, send_fn, broadcast_fn, send_context);
        break;

    case 0x12:
    case 0x13:
    case 0x14:
    case 0x15:
        handle_play_movement(connection, packet_id, reader);
        break;

    case 0x1B:
        handle_play_block_dig(connection,
                              reader,
                              socket_fd,
                              send_fn,
                              broadcast_fn,
                              send_context);
        break;

    case 0x2E:
        handle_play_block_place(connection,
                                reader,
                                socket_fd,
                                send_fn,
                                broadcast_fn,
                                send_context);
        break;

    default:
        break;
    }
}

static bool recover_player_if_out_of_world(proto_connection_t *connection,
                                           int socket_fd,
                                           proto_send_callback_t send_fn,
                                           void *send_context)
{
    ensure_world_initialized();

    double min_recovery_y = (double)s_world_config.min_y - PROTO_VOID_RECOVERY_THRESHOLD_BELOW_MIN_Y;
    if (connection->pos_y >= min_recovery_y)
    {
        return true;
    }

    int32_t world_x = (int32_t)floor(connection->pos_x);
    int32_t world_z = (int32_t)floor(connection->pos_z);
    int16_t surface_y = world_query_surface_y(&s_world_config, world_x, world_z);
    double fallback_y = (double)surface_y + PROTO_RECOVERY_SURFACE_OFFSET;
    double min_safe_y = (double)s_world_config.min_y + 2.0;
    if (fallback_y < min_safe_y)
    {
        fallback_y = min_safe_y;
    }

    connection->pos_y = fallback_y;
    connection->on_ground = false;

    reset_chunk_stream_state(connection);

    if (!send_initial_position(socket_fd, connection, send_fn, send_context))
    {
        return false;
    }

    ESP_LOGW(TAG,
             "void recovery teleport: user=%s target=(%.2f,%.2f,%.2f)",
             connection->username[0] != '\0' ? connection->username : "(unknown)",
             connection->pos_x,
             connection->pos_y,
             connection->pos_z);

    return true;
}

static void tick_survival_state(proto_connection_t *connection,
                                int socket_fd,
                                proto_send_callback_t send_fn,
                                void *send_context,
                                uint64_t now_ms)
{
    bool health_changed = false;

    ensure_world_initialized();

    if (connection->food_level > 0 && now_ms >= connection->next_hunger_decay_ms)
    {
        connection->food_level--;
        if (connection->food_level < 0)
        {
            connection->food_level = 0;
        }
        connection->next_hunger_decay_ms = now_ms + PROTO_HUNGER_DECAY_INTERVAL_MS;
        health_changed = true;
    }

    if (connection->food_level >= 18 &&
        connection->health < 20.0f &&
        now_ms >= connection->next_health_regen_ms)
    {
        connection->health += 1.0f;
        if (connection->health > 20.0f)
        {
            connection->health = 20.0f;
        }
        connection->next_health_regen_ms = now_ms + PROTO_HEALTH_REGEN_INTERVAL_MS;
        health_changed = true;
    }

    if (connection->food_level == 0 && now_ms >= connection->next_starvation_damage_ms)
    {
        connection->health -= 1.0f;
        if (connection->health < 1.0f)
        {
            connection->health = 1.0f;
        }
        connection->next_starvation_damage_ms = now_ms + PROTO_STARVATION_DAMAGE_INTERVAL_MS;
        health_changed = true;
    }

    if (connection->pos_y < (double)(s_world_config.min_y - 1) &&
        now_ms >= connection->next_void_damage_ms)
    {
        connection->health -= 2.0f;
        if (connection->health < 1.0f)
        {
            connection->health = 1.0f;
        }
        connection->next_void_damage_ms = now_ms + PROTO_VOID_DAMAGE_INTERVAL_MS;
        health_changed = true;
    }

    if (!health_changed)
    {
        return;
    }

    if (!send_update_health_packet(socket_fd, connection, send_fn, send_context))
    {
        ESP_LOGW(TAG,
                 "health update send failed: user=%s",
                 connection->username[0] != '\0' ? connection->username : "(unknown)");
    }
}

void proto_handle_packet(proto_connection_t *connection,
                         const uint8_t *packet,
                         size_t packet_length,
                         int socket_fd,
                         const proto_server_info_t *server,
                         proto_send_callback_t send_fn,
                         proto_broadcast_callback_t broadcast_fn,
                         void *send_context,
                         uint64_t now_ms)
{
    proto_reader_t reader;
    proto_reader_init(&reader, packet, packet_length);

    int32_t packet_id = 0;
    if (!proto_read_varint(&reader, &packet_id))
    {
        ESP_LOGW(TAG, "failed to read packet id");
        connection->close_requested = true;
        return;
    }

    switch (connection->state)
    {
    case PROTO_STATE_HANDSHAKE:
        if (packet_id != 0x00)
        {
            connection->close_requested = true;
            return;
        }
        handle_handshake(connection, &reader);
        break;

    case PROTO_STATE_STATUS:
        handle_status(connection, packet_id, &reader, socket_fd, server, send_fn, send_context);
        break;

    case PROTO_STATE_LOGIN:
        handle_login(connection,
                     packet_id,
                     &reader,
                     socket_fd,
                     server,
                     send_fn,
                     broadcast_fn,
                     send_context,
                     now_ms);
        break;

    case PROTO_STATE_PLAY:
        handle_play(connection,
                    packet_id,
                    &reader,
                    socket_fd,
                    send_fn,
                    broadcast_fn,
                    send_context,
                    now_ms);
        break;

    default:
        connection->close_requested = true;
        break;
    }
}

void proto_tick_connection(proto_connection_t *connection,
                           int socket_fd,
                           const proto_server_info_t *server,
                           proto_send_callback_t send_fn,
                           void *send_context,
                           uint64_t now_ms)
{
    (void)server;

    if (connection->close_requested ||
        connection->state != PROTO_STATE_PLAY ||
        !connection->joined_play)
    {
        return;
    }

    if (connection->awaiting_keepalive && now_ms >= connection->keepalive_deadline_ms)
    {
        ESP_LOGW(TAG,
                 "keepalive timeout: user=%s",
                 connection->username[0] != '\0' ? connection->username : "(unknown)");
        connection->close_requested = true;
        return;
    }

    if (!connection->awaiting_keepalive && now_ms >= connection->next_keepalive_ms)
    {
        int64_t keepalive_id = ((int64_t)now_ms << 8) ^ (int64_t)(connection->entity_id & 0xFF);
        if (!send_keepalive(socket_fd, keepalive_id, send_fn, send_context))
        {
            ESP_LOGW(TAG,
                     "keepalive send deferred: user=%s",
                     connection->username[0] != '\0' ? connection->username : "(unknown)");
            connection->next_keepalive_ms = now_ms + PROTO_KEEPALIVE_RETRY_DELAY_MS;
        }
        else
        {
            connection->last_keepalive_id = keepalive_id;
            connection->awaiting_keepalive = true;
            connection->keepalive_deadline_ms = now_ms + PROTO_KEEPALIVE_TIMEOUT_MS;
        }
    }

    if (!recover_player_if_out_of_world(connection, socket_fd, send_fn, send_context))
    {
        ESP_LOGW(TAG,
                 "void recovery failed: user=%s",
                 connection->username[0] != '\0' ? connection->username : "(unknown)");
        connection->close_requested = true;
        return;
    }

    if (connection->has_previous_chunk_center &&
        now_ms >= connection->chunk_unload_grace_until_ms)
    {
        connection->has_previous_chunk_center = false;
    }

    int32_t current_chunk_x = chunk_coord_from_position(connection->pos_x);
    int32_t current_chunk_z = chunk_coord_from_position(connection->pos_z);

    if (!connection->chunk_stream_initialized ||
        current_chunk_x != connection->chunk_center_x ||
        current_chunk_z != connection->chunk_center_z)
    {
        if (connection->chunk_stream_initialized)
        {
            connection->has_previous_chunk_center = true;
            connection->previous_chunk_center_x = connection->chunk_center_x;
            connection->previous_chunk_center_z = connection->chunk_center_z;
            connection->chunk_unload_grace_until_ms = now_ms + PROTO_CHUNK_UNLOAD_GRACE_MS;
        }

        connection->chunk_stream_initialized = true;
        connection->chunk_center_x = current_chunk_x;
        connection->chunk_center_z = current_chunk_z;
        connection->chunk_scan_index = 0;

        if (!send_update_view_position_packet(socket_fd,
                                              connection->chunk_center_x,
                                              connection->chunk_center_z,
                                              send_fn,
                                              send_context))
        {
            ESP_LOGW(TAG,
                     "view position update deferred: user=%s",
                     connection->username[0] != '\0' ? connection->username : "(unknown)");
        }
    }

    for (int32_t send_index = 0; send_index < SERVER_CHUNK_SENDS_PER_TICK; send_index++)
    {
        if (!send_next_chunk_for_connection(connection, socket_fd, send_fn, send_context))
        {
            ESP_LOGW(TAG,
                     "chunk stream tick encountered an internal failure: user=%s",
                     connection->username[0] != '\0' ? connection->username : "(unknown)");
            break;
        }
    }

    if (is_chunk_tracked(connection, connection->chunk_center_x, connection->chunk_center_z))
    {
        prune_tracked_chunks(connection, socket_fd, send_fn, send_context, now_ms);
    }

    tick_survival_state(connection, socket_fd, send_fn, send_context, now_ms);
}
