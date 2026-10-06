#include "mqtt_packet.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Read a 16-bit unsigned integer from a buffer.
 * MQTT stores multi-byte integer values in network byte order
 * (most significant byte first).
 */
static uint16_t ru16(const uint8_t *p)
{
    return (uint16_t)(((uint16_t)p[0] << 8) | p[1]);
}

/*
 * Write a 16-bit unsigned integer to a buffer
 * in network byte order (big-endian).
 */
static void wu16(uint8_t *p, uint16_t v)
{
    p[0] = (uint8_t)(v >> 8);
    p[1] = (uint8_t)v;
}

/*
 * Write an MQTT UTF-8 string to a packet buffer.
 *
 * MQTT strings are encoded as:
 *   2-byte string length
 *   string data
 *
 * Returns 0 on success and -1 if the string or buffer
 * is too large.
 */
static int utfput(uint8_t *b, size_t cap, size_t *pos, const char *s)
{
    size_t n = strlen(s);

    /* MQTT UTF-8 string length is limited to 65535 bytes. */
    if (n > 65535 || *pos + 2 + n > cap)
        return -1;

    /* Store the string length first. */
    wu16(b + *pos, (uint16_t)n);
    *pos += 2;

    /* Copy the actual string data. */
    memcpy(b + *pos, s, n);
    *pos += n;

    return 0;
}

/*
 * Read an MQTT UTF-8 string from a packet buffer.
 *
 * The decoded string is copied into 'out' and terminated
 * with a null character.
 */
static int utfget(const uint8_t *b, uint32_t l, size_t *pos,
                  char *out, size_t cap)
{
    uint16_t n;

    /* Make sure the 2-byte string length is available. */
    if (*pos + 2 > l)
        return -1;

    /* Read the string length. */
    n = ru16(b + *pos);
    *pos += 2;

    /* Check packet bounds and destination buffer size. */
    if (*pos + n > l || !cap || n >= cap)
        return -1;

    /* Copy string data and add null terminator. */
    memcpy(out, b + *pos, n);
    out[n] = 0;
    *pos += n;

    return 0;
}

/*
 * Send a complete MQTT packet.
 *
 * An MQTT packet consists of:
 *   1. Fixed header byte
 *   2. Remaining Length field
 *   3. Variable header / payload
 */
static int sendpkt(int fd, uint8_t first, const uint8_t *b, size_t n)
{
    uint8_t rl[4];
    size_t rn;

    /* Encode the packet body size as MQTT Remaining Length. */
    if (mqtt_encode_remaining_length((uint32_t)n, rl, &rn))
        return -1;

    /*
     * Send the fixed header, Remaining Length and packet body.
     * mqtt_write_full() ensures the complete requested data is sent.
     */
    if (mqtt_write_full(fd, &first, 1) ||
        mqtt_write_full(fd, rl, rn) ||
        (n && mqtt_write_full(fd, b, n)))
        return -1;

    return 0;
}

/*
 * Read one complete MQTT packet from the socket.
 *
 * 'h' receives the first fixed-header byte.
 * 'body' receives dynamically allocated packet data.
 * 'len' receives the Remaining Length value.
 */
int mqtt_read_packet(int fd, uint8_t *h, uint8_t **body, uint32_t *len)
{
    *body = NULL;
    *len = 0;

    /* Read the first fixed-header byte and Remaining Length. */
    if (mqtt_read_full(fd, h, 1) ||
        mqtt_decode_remaining_length(fd, len))
        return -1;

    /* Allocate and read the packet body if it contains data. */
    if (*len)
    {
        *body = malloc(*len);

        if (!*body || mqtt_read_full(fd, *body, *len))
        {
            free(*body);
            *body = NULL;
            return -1;
        }
    }

    return 0;
}

/*
 * Decode an MQTT CONNECT packet.
 *
 * Returns:
 *   0  = valid MQTT 5.0 CONNECT
 *  -1  = malformed packet
 *  -2  = unsupported MQTT protocol version
 */
int mqtt_decode_connect(const uint8_t *b, uint32_t l,
                        MQTT_Connect *p, char *cid, size_t cs)
{
    size_t x = 0;
    char proto[16];

    /*
     * Read and validate the Protocol Name.
     * MQTT 5.0 uses the protocol name "MQTT".
     */
    if (!b || !p || !cid ||
        utfget(b, l, &x, proto, sizeof proto) ||
        strcmp(proto, "MQTT") ||
        x + 4 > l)
        return -1;

    /* Read protocol level and CONNECT flags. */
    uint8_t level = b[x++];
    uint8_t flags = b[x++];

    /* Read the Keep Alive value. */
    p->keep_alive = ru16(b + x);
    x += 2;

    p->protocol_level = level;

    /* Clean Start is bit 1 of the CONNECT flags. */
    p->clean_start = (flags >> 1) & 1;

    /*
     * MQTT 5.0 uses protocol level 5.
     * A different level is reported separately so the broker
     * can return an Unsupported Protocol Version reason code.
     */
    if (level != 5)
        return -2;

    /* Bit 0 of CONNECT flags is reserved and must be zero. */
    if (flags & 1)
        return -1;

    if (x >= l)
        return -1;

    /*
     * Read CONNECT properties.
     * The current implementation does not decode individual
     * properties, so it skips the property field.
     */
    uint8_t pl = b[x++];

    if (x + pl > l)
        return -1;

    x += pl;

    /* Read the MQTT Client Identifier. */
    if (utfget(b, l, &x, cid, cs))
        return -1;

    return 0;
}

/*
 * Send a standard MQTT 5.0 CONNECT packet.
 */
int mqtt_send_connect(int fd, const char *client_id)
{
    return mqtt_send_connect_version(fd, client_id, 5);
}

/*
 * Send an MQTT CONNECT packet using a specified protocol level.
 *
 * Protocol level 5 represents MQTT 5.0.
 * Other values can be used for negative protocol-version tests.
 */
int mqtt_send_connect_version(int fd, const char *client_id,
                              uint8_t protocol_level)
{
    uint8_t body[256];
    size_t pos = 0;

    /* Protocol Name = "MQTT". */
    if (utfput(body, sizeof body, &pos, "MQTT"))
        return -1;

    /* Protocol Version. */
    body[pos++] = protocol_level;

    /* CONNECT flags: Clean Start = 1. */
    body[pos++] = 0x02;

    /* Keep Alive = 60 seconds. */
    wu16(body + pos, 60);
    pos += 2;

    /* CONNECT properties length = 0. */
    body[pos++] = 0;

    /* Client Identifier. */
    if (utfput(body, sizeof body, &pos, client_id))
        return -1;

    /* 0x10 = CONNECT packet type with fixed-header flags 0000. */
    return sendpkt(fd, 0x10, body, pos);
}

/*
 * Send an MQTT CONNACK packet.
 *
 * The current implementation sends:
 *   Session Present = 0
 *   Reason Code
 *   Property Length = 0
 */
int mqtt_send_connack(int fd, uint8_t reason)
{
    uint8_t body[3];

    body[0] = 0; /* Session Present = 0. */
    body[1] = reason;
    body[2] = 0; /* Property Length = 0. */

    /* 0x20 = CONNACK packet. */
    return sendpkt(fd, 0x20, body, 3);
}

/*
 * Parse an MQTT CONNACK packet received from the broker.
 */
int mqtt_parse_connack(const uint8_t *b, uint32_t l,
                       uint8_t *session_present, uint8_t *reason)
{
    if (!b || l < 2)
        return -1;

    /* Extract Session Present flag. */
    if (session_present)
        *session_present = (uint8_t)(b[0] & 1);

    /* Extract CONNACK Reason Code. */
    if (reason)
        *reason = b[1];

    return 0;
}

/*
 * Parse an MQTT SUBSCRIBE packet.
 *
 * Extracts:
 *   Packet Identifier
 *   Topic Filter
 *   Requested QoS
 */
int mqtt_parse_subscribe(const uint8_t *b, uint32_t l,
                         uint16_t *pid, char *t, size_t ts,
                         uint8_t *qos)
{
    size_t x = 0;

    if (!b || l < 3 || !pid || !t || !qos)
        return -1;

    /* First two bytes contain the Packet Identifier. */
    *pid = ru16(b);
    x = 2;

    if (x >= l)
        return -1;

    /*
     * Read the SUBSCRIBE property length and skip
     * the properties.
     */
    uint8_t pl = b[x++];

    if (x + pl > l)
        return -1;

    x += pl;

    /* Read Topic Filter and Subscription Options. */
    if (utfget(b, l, &x, t, ts) || x >= l)
        return -1;

    /* Lower two bits contain the requested QoS. */
    *qos = (uint8_t)(b[x] & 3);

    return 0;
}

/*
 * Send an MQTT SUBSCRIBE packet.
 */
int mqtt_send_subscribe(int fd, uint16_t pid,
                        const char *topic, uint8_t qos)
{
    uint8_t body[256];
    size_t pos = 0;

    /* Packet Identifier. */
    wu16(body + pos, pid);
    pos += 2;

    /* Property Length = 0. */
    body[pos++] = 0;

    /* Topic Filter. */
    if (utfput(body, sizeof body, &pos, topic))
        return -1;

    /* Subscription Options. */
    body[pos++] = (uint8_t)(qos & 3);

    /*
     * 0x82 = SUBSCRIBE packet.
     * The low nibble must be 0010 according to MQTT.
     */
    return sendpkt(fd, 0x82, body, pos);
}

/*
 * Send an MQTT SUBACK packet containing one Reason Code.
 */
int mqtt_send_suback(int fd, uint16_t pid, uint8_t reason)
{
    uint8_t body[4];

    /* Packet Identifier. */
    wu16(body, pid);

    /* Property Length = 0. */
    body[2] = 0;

    /* Subscription Reason Code. */
    body[3] = reason;

    /* 0x90 = SUBACK packet. */
    return sendpkt(fd, 0x90, body, 4);
}

/*
 * Decode an MQTT PUBLISH packet.
 *
 * The topic, QoS, DUP, RETAIN, Packet Identifier,
 * properties and payload are extracted from the packet.
 */
int mqtt_decode_publish(const uint8_t *b, uint32_t l,
                        MQTT_Publish *p, char *t, size_t ts,
                        MQTT_FixedHeader *f)
{
    size_t x = 0;

    if (!b || !p || !t || !f)
        return -1;

    /* PUBLISH begins with the Topic Name. */
    if (utfget(b, l, &x, t, ts))
        return -1;

    /*
     * Extract PUBLISH flags from the fixed header.
     *
     * Bits:
     *   bit 3 = DUP
     *   bits 2-1 = QoS
     *   bit 0 = RETAIN
     */
    p->qos = (uint8_t)((f->flags >> 1) & 3);
    p->dup = (uint8_t)((f->flags >> 3) & 1);
    p->retain = (uint8_t)(f->flags & 1);

    /* QoS 1 and QoS 2 PUBLISH packets contain Packet Identifier. */
    if (p->qos > 0)
    {
        if (x + 2 > l)
            return -1;

        p->packet_id = ru16(b + x);
        x += 2;
    }
    else
    {
        /* QoS 0 does not contain a Packet Identifier. */
        p->packet_id = 0;
    }

    /*
     * Read the PUBLISH property length and skip the
     * property data.
     */
    if (x < l)
    {
        uint8_t pl = b[x++];

        if (x + pl > l)
            return -1;

        x += pl;
    }

    /* Everything remaining in the packet is the payload. */
    p->payload = b + x;
    p->payload_len = (size_t)(l - x);

    /* Store the topic in the PUBLISH structure as well. */
    snprintf(p->topic, sizeof p->topic, "%s", t);

    return 0;
}

/*
 * Send an MQTT QoS 1 PUBLISH packet.
 */
int mqtt_send_publish(int fd, const char *t, uint16_t pid,
                      const uint8_t *p, size_t n)
{
    uint8_t body[512];
    size_t pos = 0;

    /* Topic Name. */
    if (utfput(body, sizeof body, &pos, t))
        return -1;

    /* QoS 1 PUBLISH requires a Packet Identifier. */
    wu16(body + pos, pid);
    pos += 2;

    /* Property Length = 0. */
    body[pos++] = 0;

    /* Make sure the payload fits in the packet buffer. */
    if (pos + n > sizeof body)
        return -1;

    /* Copy the payload. */
    if (n)
        memcpy(body + pos, p, n);

    pos += n;

    /*
     * QoS 1 PUBLISH:
     * Type = 3
     * DUP = 0
     * QoS = 1
     * RETAIN = 0
     *
     * Fixed header byte = 0x32.
     */
    return sendpkt(fd, 0x32, body, pos);
}

/*
 * Parse an MQTT PUBACK packet.
 */
int mqtt_parse_puback(const uint8_t *b, uint32_t l,
                      uint16_t *pid, uint8_t *reason)
{
    if (!b || l < 2 || !pid || !reason)
        return -1;

    /* Extract Packet Identifier. */
    *pid = ru16(b);

    /*
     * MQTT 5 PUBACK may contain a Reason Code.
     * If it is omitted, Success is assumed.
     */
    *reason = (l >= 3) ? b[2] : MQTT_RC_SUCCESS;

    return 0;
}

/*
 * Send an MQTT 5.0 PUBACK packet.
 */
int mqtt_send_puback(int fd, uint16_t pid, uint8_t reason)
{
    uint8_t body[4];

    /* Packet Identifier. */
    wu16(body, pid);

    /* PUBACK Reason Code. */
    body[2] = reason;

    /* Property Length = 0. */
    body[3] = 0;

    /*
     * Minimal MQTT 5 PUBACK:
     * Packet Identifier + Reason Code + Property Length.
     */
    return sendpkt(fd, 0x40, body, 3);
}

/*
 * Parse an MQTT DISCONNECT packet.
 *
 * If the packet contains no Reason Code, MQTT 5 defines
 * Normal Disconnection as the default.
 */
int mqtt_parse_disconnect(const uint8_t *b, uint32_t l,
                          uint8_t *reason)
{
    if (!reason)
        return -1;

    *reason = (b && l >= 1)
                  ? b[0]
                  : MQTT_RC_NORMAL_DISCONNECTION;

    return 0;
}

/*
 * Send an MQTT DISCONNECT packet.
 */
int mqtt_send_disconnect(int fd, uint8_t reason)
{
    uint8_t body[2];

    /* DISCONNECT Reason Code. */
    body[0] = reason;

    /* Property Length = 0. */
    body[1] = 0;

    /* 0xE0 = DISCONNECT packet. */
    return sendpkt(fd, 0xE0, body, 2);
}

/*
 * Send an MQTT UNSUBSCRIBE packet.
 */
int mqtt_send_unsubscribe(int fd, uint16_t pid, const char *topic)
{
    uint8_t body[256];
    size_t pos = 0;

    /* Packet Identifier. */
    wu16(body + pos, pid);
    pos += 2;

    /* Property Length = 0. */
    body[pos++] = 0;

    /* Topic Filter. */
    if (utfput(body, sizeof body, &pos, topic))
        return -1;

    /*
     * 0xA2 = UNSUBSCRIBE packet.
     * The low nibble is fixed at 0010.
     */
    return sendpkt(fd, 0xA2, body, pos);
}

/*
 * Parse an MQTT UNSUBSCRIBE packet.
 *
 * Extracts the Packet Identifier and first Topic Filter.
 */
int mqtt_parse_unsubscribe(const uint8_t *b, uint32_t l,
                           uint16_t *pid, char *t, size_t ts)
{
    size_t x = 0;

    if (!b || l < 3 || !pid || !t)
        return -1;

    /* Packet Identifier. */
    *pid = ru16(b);
    x = 2;

    if (x >= l)
        return -1;

    /*
     * Read and skip the UNSUBSCRIBE property field.
     */
    uint8_t pl = b[x++];

    if (x + pl > l)
        return -1;

    x += pl;

    /* Read the Topic Filter. */
    return utfget(b, l, &x, t, ts);
}

/*
 * Send an MQTT UNSUBACK packet.
 */
int mqtt_send_unsuback(int fd, uint16_t pid, uint8_t reason)
{
    uint8_t body[4];

    /* Packet Identifier. */
    wu16(body, pid);

    /* Property Length = 0. */
    body[2] = 0;

    /* UNSUBSCRIBE Reason Code. */
    body[3] = reason;

    /* 0xB0 = UNSUBACK packet. */
    return sendpkt(fd, 0xB0, body, 4);
}

/*
 * Parse an MQTT UNSUBACK packet.
 */
int mqtt_parse_unsuback(const uint8_t *b, uint32_t l,
                        uint16_t *pid, uint8_t *reason)
{
    if (!b || l < 2 || !pid || !reason)
        return -1;

    /* Packet Identifier. */
    *pid = ru16(b);

    /*
     * If a Reason Code is present, extract it.
     * Otherwise, default to Success.
     */
    *reason = (l >= 4) ? b[3] : MQTT_RC_SUCCESS;

    return 0;
}