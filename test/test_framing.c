/* Host self-check for the framing layer. proto_framing.c is portable C with no
 * ESP dependencies, so this builds and runs off-target:
 *   cl /nologo /W4 /I components/proto/include test\test_framing.c ^
 *      components\proto\proto_framing.c /Fe:test_framing.exe && test_framing.exe
 *
 * Covers the invariants that were previously only hand-traced: VarInt round-trips
 * and the read/peek max-length agreement, partial-read framing, stream compaction
 * across pipelined packets, and the oversize/invalid error split.
 */

#include <assert.h>
#include <stdio.h>
#include <string.h>

#include "proto_framing.h"

static void test_varint_roundtrip(void)
{
    static const int32_t values[] = {0, 1, 127, 128, 255, 300, 16383, 16384, 2097151, 2097152, 2147483647};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++)
    {
        uint8_t buf[16];
        proto_writer_t writer;
        proto_writer_init(&writer, buf, sizeof(buf));
        assert(proto_write_varint(&writer, values[i]));

        int32_t decoded = 0;
        proto_reader_t reader;
        proto_reader_init(&reader, buf, writer.length);
        assert(proto_read_varint(&reader, &decoded));
        assert(decoded == values[i]);
        assert(reader.offset == writer.length);

        /* peek must agree with read on value AND consumed length */
        int32_t peeked = 0;
        size_t consumed = 0;
        assert(proto_peek_varint(buf, writer.length, &peeked, &consumed));
        assert(peeked == values[i]);
        assert(consumed == writer.length);
    }
}

static void test_varint_max_length(void)
{
    /* A 5-byte VarInt is the longest legal one and must decode. */
    const uint8_t five_byte[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x07};
    int32_t value = 0;
    size_t consumed = 0;
    assert(proto_peek_varint(five_byte, sizeof(five_byte), &value, &consumed));
    assert(consumed == 5);
    assert(value == 2147483647);

    proto_reader_t reader;
    proto_reader_init(&reader, five_byte, sizeof(five_byte));
    assert(proto_read_varint(&reader, &value));

    /* A 6th continuation byte is over-long: both readers must reject it.
     * This is the invariant finding #2 flagged as hand-verified only. */
    const uint8_t six_byte[] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
    assert(!proto_peek_varint(six_byte, sizeof(six_byte), &value, &consumed));

    proto_reader_t over;
    proto_reader_init(&over, six_byte, sizeof(six_byte));
    assert(!proto_read_varint(&over, &value));

    /* Truncated VarInt reports incomplete rather than succeeding. */
    const uint8_t truncated[] = {0x80};
    assert(!proto_peek_varint(truncated, sizeof(truncated), &value, &consumed));
}

static void test_extract_partial_then_complete(void)
{
    uint8_t stream[64];
    uint8_t body[] = {0xAA, 0xBB, 0xCC};
    size_t framed_length = 0;
    assert(proto_wrap_packet(body, sizeof(body), stream, sizeof(stream), &framed_length));

    /* One byte short: must ask for more, not return a truncated packet. */
    uint8_t out[32];
    size_t stream_length = framed_length - 1;
    size_t packet_length = 0;
    assert(proto_extract_packet(stream, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_NONE);

    /* Complete it. */
    stream[framed_length - 1] = body[sizeof(body) - 1];
    stream_length = framed_length;
    assert(proto_extract_packet(stream, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_OK);
    assert(packet_length == sizeof(body));
    assert(memcmp(out, body, sizeof(body)) == 0);
    assert(stream_length == 0);
}

static void test_extract_pipelined_and_compaction(void)
{
    /* Three packets back to back in one buffer: all three must come out in
     * order and the buffer must compact to empty. */
    uint8_t stream[64];
    size_t used = 0;
    for (uint8_t i = 0; i < 3; i++)
    {
        uint8_t body[] = {i, (uint8_t)(i + 10)};
        size_t n = 0;
        assert(proto_wrap_packet(body, sizeof(body), stream + used, sizeof(stream) - used, &n));
        used += n;
    }

    uint8_t out[32];
    size_t stream_length = used;
    for (uint8_t i = 0; i < 3; i++)
    {
        size_t packet_length = 0;
        assert(proto_extract_packet(stream, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_OK);
        assert(packet_length == 2);
        assert(out[0] == i);
        assert(out[1] == i + 10);
    }
    assert(stream_length == 0);
}

static void test_extract_leftover_partial_preserved(void)
{
    /* After consuming a whole packet, a trailing partial must survive intact. */
    uint8_t stream[64];
    uint8_t body[] = {0x01, 0x02};
    size_t used = 0;
    assert(proto_wrap_packet(body, sizeof(body), stream, sizeof(stream), &used));

    /* Append the length prefix of a second, not-yet-complete packet. */
    stream[used++] = 0x02;
    stream[used++] = 0xDE;

    uint8_t out[32];
    size_t stream_length = used;
    size_t packet_length = 0;
    assert(proto_extract_packet(stream, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_OK);
    assert(packet_length == 2);
    assert(stream_length == 2);
    assert(stream[0] == 0x02 && stream[1] == 0xDE);

    /* Finish it and confirm it extracts correctly from the compacted position. */
    stream[2] = 0xAD;
    stream_length = 3;
    assert(proto_extract_packet(stream, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_OK);
    assert(packet_length == 2);
    assert(out[0] == 0xDE && out[1] == 0xAD);
    assert(stream_length == 0);
}

static void test_extract_error_split(void)
{
    uint8_t out[8];
    size_t packet_length = 0;

    /* Declares 32 bytes but capacity is 8: well-formed framing, oversized
     * packet. Must be distinguishable from malformed framing. */
    uint8_t oversize[] = {0x20, 0x01};
    size_t stream_length = sizeof(oversize);
    assert(proto_extract_packet(oversize, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_TOO_LARGE);

    /* Negative length prefix is malformed. */
    uint8_t negative[] = {0xFF, 0xFF, 0xFF, 0xFF, 0x0F};
    stream_length = sizeof(negative);
    assert(proto_extract_packet(negative, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_ERROR);

    /* Over-long VarInt is malformed. */
    uint8_t overlong[] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01};
    stream_length = sizeof(overlong);
    assert(proto_extract_packet(overlong, &stream_length, out, sizeof(out), &packet_length) == PROTO_EXTRACT_ERROR);
}

static void test_string_read(void)
{
    const char *text = "hello world";
    uint8_t buf[64];
    proto_writer_t writer;
    proto_writer_init(&writer, buf, sizeof(buf));
    assert(proto_write_string(&writer, text));

    char decoded[64];
    proto_reader_t reader;
    proto_reader_init(&reader, buf, writer.length);
    assert(proto_read_string(&reader, decoded, sizeof(decoded)));
    assert(strcmp(decoded, text) == 0);

    /* Too-small output buffer fails rather than overflowing. */
    char small[4];
    proto_reader_t tight;
    proto_reader_init(&tight, buf, writer.length);
    assert(!proto_read_string(&tight, small, sizeof(small)));
}

int main(void)
{
    test_varint_roundtrip();
    test_varint_max_length();
    test_extract_partial_then_complete();
    test_extract_pipelined_and_compaction();
    test_extract_leftover_partial_preserved();
    test_extract_error_split();
    test_string_read();
    printf("framing self-check: all assertions passed\n");
    return 0;
}