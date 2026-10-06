#ifndef MQTT_COMMON_H
#define MQTT_COMMON_H

#include <stddef.h>
#include <stdint.h>

/*
 * Default TCP port used by the MQTT broker.
 *
 * MQTT commonly uses port 1883 for non-TLS connections.
 */
#define MQTT_PORT 1883

/*
 * Maximum value allowed for the MQTT Remaining Length field.
 *
 * MQTT encodes Remaining Length using a variable-byte integer
 * occupying a maximum of four bytes. The largest valid value is:
 *
 *     268,435,455 bytes
 *
 * This constant is used when validating and encoding the field.
 */
#define MQTT_MAX_REMAINING_LENGTH 268435455U

/*
 * Read exactly 'len' bytes from a socket.
 *
 * Since a single recv() call may return fewer bytes than requested,
 * the implementation repeatedly reads until the complete buffer
 * has been received or an error occurs.
 *
 * Returns:
 *   0  - Success
 *  -1  - Connection closed or socket error
 */
int mqtt_read_full(int fd, void *buf, size_t len);

/*
 * Send exactly 'len' bytes through a socket.
 *
 * Since a single send() call may transmit fewer bytes than requested,
 * the implementation repeatedly sends until the complete buffer
 * has been transmitted or an error occurs.
 *
 * Returns:
 *   0  - Success
 *  -1  - Socket error
 */
int mqtt_write_full(int fd, const void *buf, size_t len);

/*
 * Encode an MQTT Remaining Length value using MQTT's
 * variable-byte integer format.
 *
 * Parameters:
 *   v    - Remaining Length value to encode.
 *   out  - Output buffer capable of holding up to four bytes.
 *   used - Receives the number of encoded bytes.
 *
 * Returns:
 *   0  - Encoding successful
 *  -1  - Invalid value or invalid output pointer
 */
int mqtt_encode_remaining_length(uint32_t v, uint8_t out[4], size_t *used);

/*
 * Decode an MQTT Remaining Length value received from a socket.
 *
 * The function reads up to four bytes and reconstructs the
 * Remaining Length according to the MQTT variable-byte encoding.
 *
 * Parameters:
 *   fd - Socket from which the encoded value is read.
 *   v  - Receives the decoded Remaining Length.
 *
 * Returns:
 *   0  - Decoding successful
 *  -1  - Read error or invalid encoding
 */
int mqtt_decode_remaining_length(int fd, uint32_t *v);

#endif