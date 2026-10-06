#include "mqtt_reason_codes.h"
#include <stdio.h>

const char *mqtt_reason_code_name(uint8_t code)
{
    switch (code)
    {
    case 0x00:
        return "Success / Normal disconnection / Granted QoS 0";
    case 0x01:
        return "Granted QoS 1";
    case 0x02:
        return "Granted QoS 2";
    case 0x04:
        return "Disconnect with Will Message";
    case 0x10:
        return "No matching subscribers";
    case 0x11:
        return "No subscription existed";
    case 0x18:
        return "Continue authentication";
    case 0x19:
        return "Re-authenticate";
    case 0x80:
        return "Unspecified error";
    case 0x81:
        return "Malformed Packet";
    case 0x82:
        return "Protocol Error";
    case 0x83:
        return "Implementation specific error";
    case 0x84:
        return "Unsupported Protocol Version";
    case 0x85:
        return "Client Identifier not valid";
    case 0x86:
        return "Bad User Name or Password";
    case 0x87:
        return "Not authorized";
    case 0x88:
        return "Server unavailable";
    case 0x89:
        return "Server busy";
    case 0x8A:
        return "Banned";
    case 0x8B:
        return "Server shutting down";
    case 0x8C:
        return "Bad authentication method";
    case 0x8D:
        return "Keep Alive timeout";
    case 0x8E:
        return "Session taken over";
    case 0x8F:
        return "Topic Filter invalid";
    case 0x90:
        return "Topic Name invalid";
    case 0x91:
        return "Packet Identifier in use";
    case 0x92:
        return "Packet Identifier not found";
    case 0x93:
        return "Receive Maximum exceeded";
    case 0x94:
        return "Topic Alias invalid";
    case 0x95:
        return "Packet too large";
    case 0x96:
        return "Message rate too high";
    case 0x97:
        return "Quota exceeded";
    case 0x98:
        return "Administrative action";
    case 0x99:
        return "Payload format invalid";
    case 0x9A:
        return "Retain not supported";
    case 0x9B:
        return "QoS not supported";
    case 0x9C:
        return "Use another server";
    case 0x9D:
        return "Server moved";
    case 0x9E:
        return "Shared Subscriptions not supported";
    case 0x9F:
        return "Connection rate exceeded";
    case 0xA0:
        return "Maximum connect time";
    case 0xA1:
        return "Subscription Identifiers not supported";
    case 0xA2:
        return "Wildcard Subscriptions not supported";
    default:
        return "Unknown reason code";
    }
}

void mqtt_print_reason(const char *packet_label, uint8_t code)
{
    printf("[%s] reason code : 0x%02X (%s)\n",
           packet_label ? packet_label : "MQTT",
           code,
           mqtt_reason_code_name(code));
    fflush(stdout);
}
