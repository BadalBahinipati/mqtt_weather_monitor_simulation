#include "mqtt_common.h"

#include <errno.h>
#include <sys/socket.h>
#include <unistd.h>

/*
 * Reads exactly 'len' bytes from a socket.
 *
 * recv() is not guaranteed to return all requested bytes in one call.
 * Therefore, this function keeps reading until the complete buffer has
 * been received or an error/connection closure occurs.
 *
 * Return value:
 *   0  - All requested bytes were successfully read.
 *  -1  - Connection closed or a socket error occurred.
 */
int mqtt_read_full(int fd, void *buf, size_t len)
{
    size_t n = 0;
    uint8_t *p = buf;

    while (n < len)
    {
        /*
         * Read the remaining number of bytes starting at the current
         * position in the destination buffer.
         */
        ssize_t r = recv(fd, p + n, len - n, 0);

        /*
         * recv() returning 0 means that the peer has closed the
         * connection before all requested data was received.
         */
        if (r == 0)
            return -1;

        if (r < 0)
        {
            /*
             * EINTR means the recv() call was interrupted by a signal.
             * It is safe to retry the same operation.
             */
            if (errno == EINTR)
                continue;

            /* Any other socket error is treated as a failure. */
            return -1;
        }

        /* Keep track of the total number of bytes received. */
        n += (size_t)r;
    }

    return 0;
}

/*
 * Sends exactly 'len' bytes through a socket.
 *
 * send() may transmit fewer bytes than requested, so this function
 * repeatedly calls send() until the complete buffer has been sent.
 *
 * Return value:
 *   0  - All requested bytes were successfully sent.
 *  -1  - A socket error occurred.
 */
int mqtt_write_full(int fd, const void *buf, size_t len)
{
    size_t n = 0;
    const uint8_t *p = buf;

    while (n < len)
    {
        /*
         * Send the remaining portion of the buffer starting from
         * the current byte position.
         */
        ssize_t r = send(fd, p + n, len - n, 0);

        if (r < 0)
        {
            /*
             * If the system call was interrupted by a signal,
             * retry the send operation.
             */
            if (errno == EINTR)
                continue;

            /* Any other socket error is treated as a failure. */
            return -1;
        }

        /* Update the number of bytes successfully transmitted. */
        n += (size_t)r;
    }

    return 0;
}

/*
 * Encodes the MQTT Remaining Length field.
 *
 * MQTT uses a variable-byte integer encoding for Remaining Length.
 * Each byte contains:
 *
 *   Bits 0-6 : Seven bits of the value
 *   Bit 7    : Continuation flag
 *
 * If more bytes follow, bit 7 is set to 1. The final byte has bit 7
 * cleared.
 *
 * MQTT allows a maximum Remaining Length of 268,435,455 bytes, which
 * requires at most four encoded bytes.
 *
 * Parameters:
 *   v    - Remaining Length value to encode.
 *   out  - Buffer that receives the encoded bytes.
 *   used - Receives the number of encoded bytes.
 *
 * Return value:
 *   0  - Encoding successful.
 *  -1  - Invalid value or NULL output pointer.
 */
int mqtt_encode_remaining_length(uint32_t v, uint8_t out[4], size_t *used)
{
    size_t i = 0;

    /*
     * Reject values outside the MQTT Remaining Length range and
     * reject invalid output pointers.
     */
    if (v > MQTT_MAX_REMAINING_LENGTH || !out || !used)
        return -1;

    do
    {
        /*
         * Store the lowest seven bits of the value.
         * Division by 128 moves to the next group of seven bits.
         */
        uint8_t b = (uint8_t)(v % 128U);
        v /= 128U;

        /*
         * If more data remains, set the continuation bit.
         */
        if (v)
            b |= 0x80;

        out[i++] = b;

        /*
         * MQTT Remaining Length can occupy a maximum of four bytes.
         */
    } while (v && i < 4);

    /* Return the number of bytes written to the output buffer. */
    *used = i;

    return 0;
}

/*
 * Decodes the MQTT Remaining Length field directly from a socket.
 *
 * Each byte contains seven bits of the actual value and one
 * continuation bit. The first byte represents the least significant
 * seven bits, the next byte represents the next seven bits, and so on.
 *
 * A maximum of four bytes is permitted by the MQTT specification.
 *
 * Parameters:
 *   fd - Socket from which the encoded Remaining Length is read.
 *   v  - Pointer where the decoded Remaining Length is stored.
 *
 * Return value:
 *   0  - Remaining Length successfully decoded.
 *  -1  - Socket read failure or invalid five-byte encoding.
 */
int mqtt_decode_remaining_length(int fd, uint32_t *v)
{
    /*
     * 'mult' represents the positional multiplier for each group
     * of seven bits:
     *
     *   1, 128, 16384, 2097152
     */
    uint32_t mult = 1, r = 0;

    /*
     * MQTT Remaining Length is limited to four bytes.
     */
    for (int i = 0; i < 4; i++)
    {
        uint8_t b;

        /*
         * Read one encoded byte from the socket.
         * mqtt_read_full() ensures that the complete byte is received.
         */
        if (mqtt_read_full(fd, &b, 1) != 0)
            return -1;

        /*
         * Extract the lower seven bits and add them to the decoded
         * value using the appropriate positional multiplier.
         */
        r += (uint32_t)(b & 127U) * mult;

        /*
         * Bit 7 clear means this is the final byte of the
         * Remaining Length field.
         */
        if (!(b & 128U))
        {
            *v = r;
            return 0;
        }

        /*
         * Move to the next group of seven bits for the next byte.
         */
        mult *= 128U;
    }

    /*
     * All four allowed bytes had the continuation bit set.
     * Therefore, the encoding is invalid because MQTT does not
     * permit a Remaining Length field longer than four bytes.
     */
    return -1;
}