/* Links against libelysiumkv.so and nothing else, so it fails if the pinned export
 * set (ARCHITECTURE.md "Dependencies and artifacts") is missing anything a binding needs. The static-library tests cannot
 * catch that: they see every symbol whether it is exported or not. */

#include "elysiumkv/elysiumkv.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int elysiumkv_c_smoke(const char* store_directory, const char* catalog_directory);

int main(void) {
    char root[] = "/tmp/elysiumkv-shared-XXXXXX";
    char store[256];
    char command[512];
    int result;

    if (mkdtemp(root) == NULL) {
        fprintf(stderr, "mkdtemp failed\n");
        return 1;
    }
    snprintf(store, sizeof(store), "%s/store", root);
    snprintf(command, sizeof(command), "mkdir -p %s", store);
    if (system(command) != 0) return 1;

    printf("elysiumkv_version() = %s\n", elysiumkv_version());
    result = elysiumkv_c_smoke(store, root);

    snprintf(command, sizeof(command), "rm -rf %s", root);
    (void)system(command);
    return result;
}
