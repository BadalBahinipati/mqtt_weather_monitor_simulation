CC=gcc
CFLAGS=-std=c11 -D_POSIX_C_SOURCE=200809L -D_DEFAULT_SOURCE -Wall -Wextra -Wpedantic -O2
CPPFLAGS=-Icommon -Ibroker
LDFLAGS=-pthread
COMMON=common/mqtt_common.c common/mqtt_packet.c common/mqtt_reason_codes.c
all: bin/broker bin/publisher bin/subscriber
bin:
	mkdir -p bin
bin/broker: broker/broker.c broker/broker_state.c broker/client_handler.c $(COMMON) | bin
	$(CC) $(CFLAGS) $(CPPFLAGS) $^ -o $@ $(LDFLAGS)
bin/publisher: publisher/publisher.c $(COMMON) | bin
	$(CC) $(CFLAGS) $(CPPFLAGS) $^ -o $@ $(LDFLAGS)
bin/subscriber: subscriber/subscriber.c $(COMMON) | bin
	$(CC) $(CFLAGS) $(CPPFLAGS) $^ -o $@ $(LDFLAGS)
clean:
	rm -f bin/broker bin/publisher bin/subscriber
.PHONY: all clean
