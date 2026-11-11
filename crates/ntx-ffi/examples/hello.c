/*
 * Minimal NTX example in C: start a source, connect a receiver, send one line of text and
 * print what the receiver gets. Error handling is kept short for clarity.
 *
 * Build (from the repository root, after `cargo build -p ntx-ffi`):
 *   cc crates/ntx-ffi/examples/hello.c -I crates/ntx-ffi/include -L target/debug -lntx_ffi -o hello
 */
#include <stdio.h>
#include <string.h>

#include "ntx.h"

static int fail(const char *what) {
    fprintf(stderr, "%s failed: %s\n", what, ntx_last_error());
    return 1;
}

int main(void) {
    NtxSource *source = NULL;
    const char *config = "{\"name\": \"C (hello)\", \"bind\": \"127.0.0.1\", \"port\": 0, \"advertise\": false}";
    if (ntx_source_start(config, &source) != NTX_OK) return fail("ntx_source_start");

    /* The info JSON looks like {"address":"127.0.0.1:54321",...}: build ws://<address>/ from it. */
    char *info = ntx_source_info(source);
    char url[64] = "ws://";
    const char *address = strstr(info, "\"address\":\"") + strlen("\"address\":\"");
    strncat(url, address, strcspn(address, "\""));
    strcat(url, "/");
    ntx_string_free(info);

    NtxReceiver *receiver = NULL;
    if (ntx_receiver_connect(url, "{\"name\": \"C (receiver)\"}", &receiver) != NTX_OK) {
        return fail("ntx_receiver_connect");
    }
    if (ntx_source_update_text(source, "program", "Hello from C") != NTX_OK) return fail("update");

    /* Poll until the receiver has the text. */
    int found = 0;
    for (int i = 0; i < 50 && !found; i++) {
        char *event = ntx_receiver_wait_event(receiver, 100);
        if (event != NULL) {
            found = strstr(event, "\"event\":\"update\"") && strstr(event, "Hello from C");
            if (found) printf("%s\n", event);
            ntx_string_free(event);
        }
    }

    ntx_receiver_free(receiver);
    ntx_source_free(source);
    printf("ntx %s, protocol %s: %s\n", ntx_version(), ntx_protocol_version(), found ? "ok" : "no update");
    return found ? 0 : 1;
}
