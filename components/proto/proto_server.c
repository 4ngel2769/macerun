#include "proto_server.h"

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "proto_framing.h"
#include "server_limits.h"

#define PROTO_KEEPALIVE_INTERVAL_MS 5000ULL
#define PROTO_KEEPALIVE_TIMEOUT_MS 15000ULL

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
} nbt_tag_t;

static int32_t s_next_entity_id = 1;

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
    uint8_t framed[SERVER_MAX_PACKET_SIZE + 8];
    size_t framed_length = 0;

    if (!proto_wrap_packet(packet_body, packet_length, framed, sizeof(framed), &framed_length))
    {
        return false;
    }

    return send_fn(send_context, socket_fd, framed, framed_length);
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

    uint8_t framed[SERVER_MAX_PACKET_SIZE + 8];
    size_t framed_length = 0;

    if (!proto_wrap_packet(packet_body, packet_length, framed, sizeof(framed), &framed_length))
    {
        return false;
    }

    return broadcast_fn(send_context, source_socket_fd, framed, framed_length);
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

    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x00))
    {
        return false;
    }
    if (!proto_write_string(&writer, json))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool send_pong(int socket_fd,
                      int64_t payload,
                      proto_send_callback_t send_fn,
                      void *send_context)
{
    uint8_t packet[16];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x01))
    {
        return false;
    }
    if (!proto_write_i64_be(&writer, payload))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
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

    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x00))
    {
        return false;
    }
    if (!proto_write_string(&writer, json))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool send_login_success(int socket_fd,
                               const proto_connection_t *connection,
                               proto_send_callback_t send_fn,
                               void *send_context)
{
    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x02) ||
        !proto_write_i64_be(&writer, connection->uuid_most) ||
        !proto_write_i64_be(&writer, connection->uuid_least) ||
        !proto_write_string(&writer, connection->username))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool send_play_login(int socket_fd,
                            const proto_connection_t *connection,
                            const proto_server_info_t *server,
                            proto_send_callback_t send_fn,
                            void *send_context)
{
    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

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
        !proto_write_varint(&writer, 10) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_u8(&writer, 1) ||
        !proto_write_u8(&writer, 0) ||
        !proto_write_u8(&writer, 0))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool send_initial_position(int socket_fd,
                                  const proto_connection_t *connection,
                                  proto_send_callback_t send_fn,
                                  void *send_context)
{
    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

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

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool send_keepalive(int socket_fd,
                           int64_t keepalive_id,
                           proto_send_callback_t send_fn,
                           void *send_context)
{
    uint8_t packet[32];
    proto_writer_t writer;
    proto_writer_init(&writer, packet, sizeof(packet));

    if (!proto_write_varint(&writer, 0x1F) ||
        !proto_write_i64_be(&writer, keepalive_id))
    {
        return false;
    }

    return send_packet(socket_fd, packet, writer.length, send_fn, send_context);
}

static bool build_chat_packet_body(const char *message_text,
                                   int64_t uuid_most,
                                   int64_t uuid_least,
                                   uint8_t *packet,
                                   size_t packet_capacity,
                                   size_t *packet_length)
{
    char escaped_message[256];
    if (!escape_json_text(message_text, escaped_message, sizeof(escaped_message)))
    {
        return false;
    }

    char json[300];
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
    uint8_t packet_body[SERVER_MAX_PACKET_SIZE];
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
    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    size_t packet_length = 0;

    if (!build_chat_packet_body(message_text,
                                uuid_most,
                                uuid_least,
                                packet,
                                sizeof(packet),
                                &packet_length))
    {
        return false;
    }

    return send_packet(socket_fd, packet, packet_length, send_fn, send_context);
}

static bool broadcast_chat_text(int source_socket_fd,
                                const char *message_text,
                                int64_t uuid_most,
                                int64_t uuid_least,
                                proto_broadcast_callback_t broadcast_fn,
                                void *send_context)
{
    uint8_t packet[SERVER_MAX_PACKET_SIZE];
    size_t packet_length = 0;

    if (!build_chat_packet_body(message_text,
                                uuid_most,
                                uuid_least,
                                packet,
                                sizeof(packet),
                                &packet_length))
    {
        return false;
    }

    return broadcast_packet(source_socket_fd, packet, packet_length, broadcast_fn, send_context);
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
            connection->close_requested = true;
        }
        return;
    }

    if (packet_id == 0x01)
    {
        int64_t payload = 0;
        if (!proto_read_i64_be(reader, &payload))
        {
            connection->close_requested = true;
            return;
        }

        if (!send_pong(socket_fd, payload, send_fn, send_context))
        {
            connection->close_requested = true;
            return;
        }

        connection->close_requested = true;
        return;
    }

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
        connection->close_requested = true;
        return;
    }

    if (!proto_read_string(reader, connection->username, sizeof(connection->username)))
    {
        connection->close_requested = true;
        return;
    }

    if (connection->username[0] == '\0')
    {
        send_login_disconnect(socket_fd, "Username is required.", send_fn, send_context);
        connection->close_requested = true;
        return;
    }

    connection->entity_id = next_entity_id();
    generate_offline_uuid(connection->username, &connection->uuid_most, &connection->uuid_least);

    if (!send_login_success(socket_fd, connection, send_fn, send_context))
    {
        connection->close_requested = true;
        return;
    }

    connection->state = PROTO_STATE_PLAY;
    connection->joined_play = true;
    connection->awaiting_keepalive = false;
    connection->next_keepalive_ms = now_ms + PROTO_KEEPALIVE_INTERVAL_MS;
    connection->last_activity_ms = now_ms;

    if (!send_play_login(socket_fd, connection, server, send_fn, send_context) ||
        !send_initial_position(socket_fd, connection, send_fn, send_context))
    {
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
        connection->close_requested = true;
        return;
    }

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
        connection->close_requested = true;
        return;
    }

    if (!connection->awaiting_keepalive || keepalive_id != connection->last_keepalive_id)
    {
        connection->close_requested = true;
        return;
    }

    connection->awaiting_keepalive = false;
    connection->keepalive_deadline_ms = 0;
    connection->next_keepalive_ms = now_ms + PROTO_KEEPALIVE_INTERVAL_MS;
}

static void handle_play_chat(proto_connection_t *connection,
                             proto_reader_t *reader,
                             int socket_fd,
                             proto_send_callback_t send_fn,
                             proto_broadcast_callback_t broadcast_fn,
                             void *send_context)
{
    char chat_message[256];
    if (!proto_read_string(reader, chat_message, sizeof(chat_message)))
    {
        connection->close_requested = true;
        return;
    }

    if (chat_message[0] == '\0')
    {
        return;
    }

    if (chat_message[0] == '/')
    {
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

    char line[288];
    int written = snprintf(line, sizeof(line), "<%s> %s", connection->username, chat_message);
    if (written <= 0 || written >= (int)sizeof(line))
    {
        return;
    }

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

    default:
        break;
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
        connection->close_requested = true;
        return;
    }

    if (!connection->awaiting_keepalive && now_ms >= connection->next_keepalive_ms)
    {
        int64_t keepalive_id = ((int64_t)now_ms << 8) ^ (int64_t)(connection->entity_id & 0xFF);
        if (!send_keepalive(socket_fd, keepalive_id, send_fn, send_context))
        {
            connection->close_requested = true;
            return;
        }

        connection->last_keepalive_id = keepalive_id;
        connection->awaiting_keepalive = true;
        connection->keepalive_deadline_ms = now_ms + PROTO_KEEPALIVE_TIMEOUT_MS;
    }
}
