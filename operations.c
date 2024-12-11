#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

static int compare_keys(const void *a, const void *b) {
    const char *key_a = *(const char **)a;
    const char *key_b = *(const char **)b;
    return strcmp(key_a, key_b);
}


int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    // Copiar as chaves para um array de ponteiros para facilitar a ordenação
    char *sorted_keys[num_pairs];
    for (size_t i = 0; i < num_pairs; i++) {
        sorted_keys[i] = keys[i];
    }

    // Ordenar as chaves
    qsort(sorted_keys, num_pairs, sizeof(char *), compare_keys);

    int has_errors = 0; // Indica se há erros para abrir os parênteses retos

    for (size_t i = 0; i < num_pairs; i++) {
        char *value = read_pair(kvs_table, sorted_keys[i]);
        if (value == NULL) {
            if (!has_errors) {
                // Abre os parênteses retos na primeira ocorrência de erro
                dprintf(out_fd, "[");
                has_errors = 1;
            }
            dprintf(out_fd, "(%s,KVSERROR)", sorted_keys[i]);
        } else {
            free(value); // Liberar memória de chaves válidas
        }

        if (value == NULL && i < num_pairs - 1) {
            // Adiciona espaços apenas para erros antes do último item
        }
    }

    if (has_errors) {
        // Fecha os parênteses retos se houve erros
        dprintf(out_fd, "]\n");
    }

    return 0;
}



int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    int has_errors = 0; // Indica se há erros para abrir os parênteses retos

    for (size_t i = 0; i < num_pairs; i++) {
        if (delete_pair(kvs_table, keys[i]) != 0) {
            if (!has_errors) {
                // Abre os parênteses retos na primeira ocorrência de erro
                dprintf(out_fd, "[");
                has_errors = 1;
            }
            dprintf(out_fd, "(%s,KVSMISSING)", keys[i]);
            if (i < num_pairs - 1) {
            }
        }
    }

    if (has_errors) {
        // Fecha os parênteses retos se houve erros
        dprintf(out_fd, "]\n");
    }

    return 0;
}



void kvs_show(int fd) {
    if (kvs_table == NULL) {
        dprintf(fd, "KVS not initialized\n");
        return;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *node = kvs_table->table[i];
        while (node) {
            dprintf(fd, "(%s, %s)\n", node->key, node->value);
            node = node->next;
        }
    }
}


int kvs_backup(const char *backup_file) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    int fd = open(backup_file, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd == -1) {
        perror("Error opening backup file");
        return 1;
    }

    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *node = kvs_table->table[i];
        while (node) {
            dprintf(fd, "%s=%s\n", node->key, node->value);
            node = node->next;
        }
    }

    close(fd);
    return 0;
}


void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}