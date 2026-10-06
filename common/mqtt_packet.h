#ifndef MQTT_PACKET_H
#define MQTT_PACKET_H

#include "mqtt_common.h"
#include "mqtt_reason_codes.h"

#include <stddef.h>
#include <stdint.h>

/*
 * MQTT Control Packet types.
 *
 * The packet type is stored in the upper four bits
 * of the MQTT Fixed Header.
 */
enum
{
    MQTT_CONNECT = 1,
    MQTT_CONNACK = 2,
    MQTT_PUBLISH = 3,
    MQTT_PUBACK = 4,
    MQTT_SUBSCRIBE = 8,
    MQTT_SUBACK = 9,
    MQTT_UNSUBSCRIBE = 10,
    MQTT_UNSUBACK = 11,
    MQTT_DISCONNECT = 14
};

/*
 * MQTT Fixed Header.
 *
 * Every MQTT Control Packet contains a Fixed Header.
 *
 * packet_type:
 *     Identifies the MQTT Control Packet type.
 *
 * flags:
 *     Contains the packet-specific flags from the
 *     lower four bits of the first header byte.
 *
 * remaining_length:
 *     Number of bytes following the Remaining Length field.
 */
typedef struct
{
    uint8_t packet_type, flags;
    uint32_t remaining_length;

} MQTT_FixedHeader;

/*
 * MQTT CONNECT packet information.
 *
 * protocol_level:
 *     MQTT protocol version. MQTT 5.0 uses value 5.
 *
 * clean_start:
 *     Indicates whether the client requests a new session.
 *
 * keep_alive:
 *     Maximum time interval, in seconds, between client
 *     control packets.
 */
typedef struct
{
    uint8_t protocol_level, clean_start;
    uint16_t keep_alive;

} MQTT_Connect;

/*
 * MQTT CONNACK packet information.
 *
 * session_present:
 *     Indicates whether an existing session is present.
 *
 * reason_code:
 *     Result of the CONNECT request.
 */
typedef struct
{
    uint8_t session_present, reason_code;

} MQTT_ConnAck;

/*
 * MQTT SUBSCRIBE packet information.
 *
 * packet_id:
 *     Identifier used to match the SUBSCRIBE with its SUBACK.
 *
 * topic_filter:
 *     Topic filter requested by the subscriber.
 *
 * requested_qos:
 *     Maximum QoS requested for the subscription.
 */
typedef struct
{
    uint16_t packet_id;
    char topic_filter[128];
    uint8_t requested_qos;

} MQTT_Subscribe;

/*
 * MQTT SUBACK packet information.
 *
 * packet_id:
 *     Packet Identifier copied from the corresponding
 *     SUBSCRIBE packet.
 *
 * reason_code:
 *     Result of the subscription request.
 */
typedef struct
{
    uint16_t packet_id;
    uint8_t reason_code;

} MQTT_SubAck;

/*
 * MQTT PUBLISH packet information.
 *
 * packet_id:
 *     Identifier used for QoS 1 or QoS 2 PUBLISH packets.
 *     It is not used for QoS 0.
 *
 * topic:
 *     Topic Name associated with the published message.
 *
 * payload:
 *     Pointer to the actual published data.
 *
 * payload_len:
 *     Number of bytes in the payload.
 *
 * qos:
 *     Quality of Service level.
 *
 * dup:
 *     DUP flag from the PUBLISH Fixed Header.
 *
 * retain:
 *     RETAIN flag from the PUBLISH Fixed Header.
 */
typedef struct
{
    uint16_t packet_id;
    char topic[128];

    /*
     * The payload is not copied into this structure.
     * It points directly into the received packet buffer.
     */
    const uint8_t *payload;

    size_t payload_len;

    uint8_t qos, dup, retain;

} MQTT_Publish;

/*
 * MQTT PUBACK packet information.
 *
 * packet_id:
 *     Identifier of the PUBLISH packet being acknowledged.
 *
 * reason_code:
 *     Result of processing the PUBLISH packet.
 */
typedef struct
{
    uint16_t packet_id;
    uint8_t reason_code;

} MQTT_PubAck;

/*
 * MQTT DISCONNECT packet information.
 *
 * reason_code:
 *     Reason for closing the MQTT connection.
 */
typedef struct
{
    uint8_t reason_code;

} MQTT_Disconnect;

/*
 * MQTT UNSUBSCRIBE packet information.
 *
 * packet_id:
 *     Identifier used to match the UNSUBSCRIBE with its
 *     corresponding UNSUBACK.
 *
 * topic_filter:
 *     Topic filter from which the client wants to unsubscribe.
 */
typedef struct
{
    uint16_t packet_id;
    char topic_filter[128];

} MQTT_Unsubscribe;

/*
 * MQTT UNSUBACK packet information.
 *
 * packet_id:
 *     Identifier copied from the UNSUBSCRIBE packet.
 *
 * reason_code:
 *     Result of the unsubscribe request.
 */
typedef struct
{
    uint16_t packet_id;
    uint8_t reason_code;

} MQTT_UnsubAck;

/*
 * Read one complete MQTT packet from a socket.
 *
 * h:
 *     Receives the first byte of the Fixed Header.
 *
 * body:
 *     Receives dynamically allocated packet body data.
 *
 * len:
 *     Receives the MQTT Remaining Length.
 */
int mqtt_read_packet(int fd, uint8_t *h, uint8_t **body, uint32_t *len);

/*
 * Decode the variable header and payload of an MQTT CONNECT packet.
 *
 * cid:
 *     Receives the Client Identifier.
 *
 * cs:
 *     Size of the Client Identifier buffer.
 */
int mqtt_decode_connect(const uint8_t *b, uint32_t l,
                        MQTT_Connect *p, char *cid, size_t cs);

/*
 * Send a standard MQTT 5.0 CONNECT packet.
 */
int mqtt_send_connect(int fd, const char *client_id);

/*
 * Send an MQTT CONNECT packet using the specified protocol level.
 *
 * Protocol level 5 represents MQTT 5.0.
 * Other values can be used for protocol-version testing.
 */
int mqtt_send_connect_version(int fd, const char *client_id,
                              uint8_t protocol_level);

/*
 * Send an MQTT CONNACK packet with the specified Reason Code.
 */
int mqtt_send_connack(int fd, uint8_t reason);

/*
 * Parse an MQTT CONNACK packet.
 *
 * Extracts the Session Present flag and Reason Code.
 */
int mqtt_parse_connack(const uint8_t *b, uint32_t l,
                       uint8_t *session_present, uint8_t *reason);

/*
 * Parse an MQTT SUBSCRIBE packet.
 *
 * Extracts the Packet Identifier, Topic Filter and
 * requested QoS.
 */
int mqtt_parse_subscribe(const uint8_t *b, uint32_t l,
                         uint16_t *pid, char *t, size_t ts,
                         uint8_t *qos);

/*
 * Send an MQTT SUBSCRIBE packet.
 */
int mqtt_send_subscribe(int fd, uint16_t pid,
                        const char *topic, uint8_t qos);

/*
 * Send an MQTT SUBACK packet containing the specified
 * subscription Reason Code.
 */
int mqtt_send_suback(int fd, uint16_t pid, uint8_t reason);

/*
 * Decode an MQTT PUBLISH packet.
 *
 * Extracts the Topic Name, Packet Identifier when applicable,
 * QoS, DUP flag, RETAIN flag, properties and payload.
 */
int mqtt_decode_publish(const uint8_t *b, uint32_t l,
                        MQTT_Publish *p, char *t, size_t ts,
                        MQTT_FixedHeader *f);

/*
 * Send an MQTT QoS 1 PUBLISH packet.
 */
int mqtt_send_publish(int fd, const char *t, uint16_t pid,
                      const uint8_t *p, size_t n);

/*
 * Parse an MQTT PUBACK packet.
 *
 * Extracts the Packet Identifier and Reason Code.
 */
int mqtt_parse_puback(const uint8_t *b, uint32_t l,
                      uint16_t *pid, uint8_t *reason);

/*
 * Send an MQTT PUBACK packet.
 */
int mqtt_send_puback(int fd, uint16_t pid, uint8_t reason);

/*
 * Parse an MQTT DISCONNECT packet.
 *
 * Extracts the Disconnect Reason Code.
 */
int mqtt_parse_disconnect(const uint8_t *b, uint32_t l,
                          uint8_t *reason);

/*
 * Send an MQTT DISCONNECT packet.
 */
int mqtt_send_disconnect(int fd, uint8_t reason);

/*
 * Send an MQTT UNSUBSCRIBE packet for the specified topic filter.
 */
int mqtt_send_unsubscribe(int fd, uint16_t pid,
                          const char *topic);

/*
 * Parse an MQTT UNSUBSCRIBE packet.
 *
 * Extracts the Packet Identifier and Topic Filter.
 */
int mqtt_parse_unsubscribe(const uint8_t *b, uint32_t l,
                           uint16_t *pid, char *t, size_t ts);

/*
 * Send an MQTT UNSUBACK packet with the specified Reason Code.
 */
int mqtt_send_unsuback(int fd, uint16_t pid, uint8_t reason);

/*
 * Parse an MQTT UNSUBACK packet.
 *
 * Extracts the Packet Identifier and Reason Code.
 */
int mqtt_parse_unsuback(const uint8_t *b, uint32_t l,
                        uint16_t *pid, uint8_t *reason);

#endif