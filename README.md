# mqtt_final_v4

Custom MQTT 5.0 training implementation in C using POSIX IPv4 TCP sockets. No MQTT library is used.

## Build

```bash
make
```

## Run

```bash
# Terminal 1 — broker stays up until you choose Exit
./bin/broker

# Terminal 2 — subscriber connects, shows menu, executes one choice, then exits
./bin/subscriber 127.0.0.1

# Terminal 3 — publisher connects, shows menu, executes one choice, then exits
./bin/publisher 127.0.0.1
```

Default port: **1883**.

Recommended order for sensor data: start broker → start subscriber (request temp/humidity) → start publisher (send valid data).

## Behaviour

| Program    | Lifetime |
|-----------|----------|
| Broker    | Runs until **2. Exit** (or SIGINT/SIGTERM). Start/Stop toggles listening. |
| Publisher | CONNECT → menu → execute one choice → DISCONNECT → exit. |
| Subscriber| CONNECT → menu → execute one choice → DISCONNECT → exit. |

## Menus

### Publisher
1. Send valid sensor data (temperature + humidity)
2. Send invalid sensor data → PUBACK **0x99**
3. Exit

### Subscriber
1. Request Temperature
2. Request Humidity
3. Request Temperature & Humidity
4. Invalid Request → PUBACK **0x83**
5. Invalid Topic → PUBACK **0x90**
6. Invalid Topic Filter → SUBACK **0x8F**
7. Unsupported Version → CONNACK **0x84**
8. Invalid Payload → PUBACK **0x99**
9. Disconnect
10. Exit

If the publisher sends invalid data, the broker does not forward it. A waiting subscriber times out, prints an error, and terminates.

## Broker dashboard

Pinned at the top (TTY). Shows:

- Broker status (RUNNING / STOPPED)
- Publishers count
- Subscribers count
- Last published temperature
- Last published humidity

Menu: **1. Start/Stop**, **2. Exit**. Frame is drawn once; live fields update in place.

## Reason codes

Clients print CONNECT, CONNACK, SUBSCRIBE, SUBACK, PUBLISH, PUBACK, and DISCONNECT with MQTT 5.0 reason codes, for example:

```text
[PUBACK] reason code : 0x99 (Payload format invalid)
  [ERROR] rejected by broker
```

## Cleanup

- Publisher and subscriber send DISCONNECT on exit / signal.
- Broker joins accept and client threads on Stop/Exit/signal.
