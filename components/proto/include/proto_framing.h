#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct
{
    const uint8_t *data;
    size_t length;
    size_t offset;
} proto_reader_t;

typedef struct
{
    uint8_t *data;
    size_t capacity;
    size_t length;
} proto_writer_t;

typedef enum
{
    PROTO_EXTRACT_NONE = 0,
    PROTO_EXTRACT_OK = 1,
    PROTO_EXTRACT_ERROR = -1,
} proto_extract_result_t;

void proto_reader_init(proto_reader_t *reader, const uint8_t *data, size_t length);
bool proto_read_u8(proto_reader_t *reader, uint8_t *value);
bool proto_read_u16_be(proto_reader_t *reader, uint16_t *value);
bool proto_read_i64_be(proto_reader_t *reader, int64_t *value);
bool proto_read_varint(proto_reader_t *reader, int32_t *value);
bool proto_read_string(proto_reader_t *reader, char *out, size_t out_size);

void proto_writer_init(proto_writer_t *writer, uint8_t *data, size_t capacity);
bool proto_write_u8(proto_writer_t *writer, uint8_t value);
bool proto_write_u16_be(proto_writer_t *writer, uint16_t value);
bool proto_write_i64_be(proto_writer_t *writer, int64_t value);
bool proto_write_bytes(proto_writer_t *writer, const uint8_t *data, size_t length);
bool proto_write_varint(proto_writer_t *writer, int32_t value);
bool proto_write_string(proto_writer_t *writer, const char *value);

bool proto_peek_varint(const uint8_t *data, size_t data_length, int32_t *value, size_t *consumed);

proto_extract_result_t proto_extract_packet(uint8_t *stream,
                                            size_t *stream_length,
                                            uint8_t *packet_out,
                                            size_t packet_capacity,
                                            size_t *packet_length);

bool proto_wrap_packet(const uint8_t *packet_body,
                       size_t packet_length,
                       uint8_t *framed_out,
                       size_t framed_capacity,
                       size_t *framed_length);
