#include "proto_framing.h"

#include <limits.h>
#include <string.h>

void proto_reader_init(proto_reader_t *reader, const uint8_t *data, size_t length)
{
    reader->data = data;
    reader->length = length;
    reader->offset = 0;
}

bool proto_read_u8(proto_reader_t *reader, uint8_t *value)
{
    if (reader->offset + 1 > reader->length)
    {
        return false;
    }

    *value = reader->data[reader->offset++];
    return true;
}

bool proto_read_u16_be(proto_reader_t *reader, uint16_t *value)
{
    if (reader->offset + 2 > reader->length)
    {
        return false;
    }

    uint16_t high = reader->data[reader->offset++];
    uint16_t low = reader->data[reader->offset++];
    *value = (uint16_t)((high << 8) | low);
    return true;
}

bool proto_read_i64_be(proto_reader_t *reader, int64_t *value)
{
    if (reader->offset + 8 > reader->length)
    {
        return false;
    }

    uint64_t result = 0;
    for (size_t i = 0; i < 8; i++)
    {
        result = (result << 8) | reader->data[reader->offset++];
    }

    *value = (int64_t)result;
    return true;
}

bool proto_read_varint(proto_reader_t *reader, int32_t *value)
{
    int32_t result = 0;
    uint8_t shift = 0;

    while (true)
    {
        if (reader->offset >= reader->length)
        {
            return false;
        }

        uint8_t current = reader->data[reader->offset++];
        result |= (int32_t)(current & 0x7F) << shift;

        if ((current & 0x80) == 0)
        {
            *value = result;
            return true;
        }

        shift += 7;
        if (shift >= 35)
        {
            return false;
        }
    }
}

bool proto_read_string(proto_reader_t *reader, char *out, size_t out_size)
{
    int32_t length = 0;
    if (!proto_read_varint(reader, &length))
    {
        return false;
    }

    if (length < 0)
    {
        return false;
    }

    size_t str_len = (size_t)length;
    if (reader->offset + str_len > reader->length || str_len + 1 > out_size)
    {
        return false;
    }

    memcpy(out, reader->data + reader->offset, str_len);
    out[str_len] = '\0';
    reader->offset += str_len;
    return true;
}

void proto_writer_init(proto_writer_t *writer, uint8_t *data, size_t capacity)
{
    writer->data = data;
    writer->capacity = capacity;
    writer->length = 0;
}

bool proto_write_u8(proto_writer_t *writer, uint8_t value)
{
    if (writer->length + 1 > writer->capacity)
    {
        return false;
    }

    writer->data[writer->length++] = value;
    return true;
}

bool proto_write_u16_be(proto_writer_t *writer, uint16_t value)
{
    if (writer->length + 2 > writer->capacity)
    {
        return false;
    }

    writer->data[writer->length++] = (uint8_t)(value >> 8);
    writer->data[writer->length++] = (uint8_t)(value & 0xFF);
    return true;
}

bool proto_write_i64_be(proto_writer_t *writer, int64_t value)
{
    if (writer->length + 8 > writer->capacity)
    {
        return false;
    }

    uint64_t raw = (uint64_t)value;
    for (int8_t i = 7; i >= 0; i--)
    {
        writer->data[writer->length + i] = (uint8_t)(raw & 0xFF);
        raw >>= 8;
    }

    writer->length += 8;
    return true;
}

bool proto_write_bytes(proto_writer_t *writer, const uint8_t *data, size_t length)
{
    if (writer->length + length > writer->capacity)
    {
        return false;
    }

    memcpy(writer->data + writer->length, data, length);
    writer->length += length;
    return true;
}

bool proto_write_varint(proto_writer_t *writer, int32_t value)
{
    uint32_t encoded = (uint32_t)value;

    while (true)
    {
        uint8_t byte = (uint8_t)(encoded & 0x7F);
        encoded >>= 7;

        if (encoded != 0)
        {
            byte |= 0x80;
        }

        if (!proto_write_u8(writer, byte))
        {
            return false;
        }

        if (encoded == 0)
        {
            return true;
        }
    }
}

bool proto_write_string(proto_writer_t *writer, const char *value)
{
    size_t length = strlen(value);
    if (length > INT32_MAX)
    {
        return false;
    }

    if (!proto_write_varint(writer, (int32_t)length))
    {
        return false;
    }

    return proto_write_bytes(writer, (const uint8_t *)value, length);
}

bool proto_peek_varint(const uint8_t *data, size_t data_length, int32_t *value, size_t *consumed)
{
    int32_t result = 0;
    uint8_t shift = 0;

    for (size_t i = 0; i < data_length && i < 5; i++)
    {
        uint8_t current = data[i];
        result |= (int32_t)(current & 0x7F) << shift;

        if ((current & 0x80) == 0)
        {
            *value = result;
            *consumed = i + 1;
            return true;
        }

        shift += 7;
    }

    return false;
}

proto_extract_result_t proto_extract_packet(uint8_t *stream,
                                            size_t *stream_length,
                                            uint8_t *packet_out,
                                            size_t packet_capacity,
                                            size_t *packet_length)
{
    int32_t body_length = 0;
    size_t varint_len = 0;

    if (!proto_peek_varint(stream, *stream_length, &body_length, &varint_len))
    {
        if (*stream_length >= 5)
        {
            return PROTO_EXTRACT_ERROR;
        }
        return PROTO_EXTRACT_NONE;
    }

    if (body_length < 0 || (size_t)body_length > packet_capacity)
    {
        return PROTO_EXTRACT_ERROR;
    }

    size_t total = varint_len + (size_t)body_length;
    if (*stream_length < total)
    {
        return PROTO_EXTRACT_NONE;
    }

    memcpy(packet_out, stream + varint_len, (size_t)body_length);
    *packet_length = (size_t)body_length;

    size_t remaining = *stream_length - total;
    if (remaining > 0)
    {
        memmove(stream, stream + total, remaining);
    }
    *stream_length = remaining;

    return PROTO_EXTRACT_OK;
}

bool proto_wrap_packet(const uint8_t *packet_body,
                       size_t packet_length,
                       uint8_t *framed_out,
                       size_t framed_capacity,
                       size_t *framed_length)
{
    proto_writer_t writer;
    proto_writer_init(&writer, framed_out, framed_capacity);

    if (packet_length > INT32_MAX)
    {
        return false;
    }

    if (!proto_write_varint(&writer, (int32_t)packet_length))
    {
        return false;
    }

    if (!proto_write_bytes(&writer, packet_body, packet_length))
    {
        return false;
    }

    *framed_length = writer.length;
    return true;
}
