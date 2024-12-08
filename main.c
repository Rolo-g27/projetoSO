#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include "job_processor.h"
#include "constants.h"
#include "parser.h"
#include "operations.h"

void process_directory(const char *job_directory);

int main(int argc, char *argv[]) {
    // Inicializar o sistema KVS
    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    // Verificar se o diretório foi fornecido como argumento
    if (argc == 2) {
        // Processar todos os arquivos .job no diretório fornecido
        process_directory(argv[1]);
        kvs_terminate();
        return 0;
    }

    // Modo interativo (terminal)
    while (1) {
        char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
        unsigned int delay;
        size_t num_pairs;

        printf("> ");
        fflush(stdout);

        switch (get_next(STDIN_FILENO)) {
            case CMD_WRITE:
                num_pairs = parse_write(STDIN_FILENO, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pair\n");
                }

                break;

            case CMD_READ:
                num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_read(num_pairs, keys)) {
                    fprintf(stderr, "Failed to read pair\n");
                }
                break;

            case CMD_DELETE:
                num_pairs = parse_read_delete(STDIN_FILENO, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

                if (num_pairs == 0) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_delete(num_pairs, keys)) {
                    fprintf(stderr, "Failed to delete pair\n");
                }
                break;

            case CMD_SHOW:
                kvs_show();
                break;

            case CMD_WAIT:
                if (parse_wait(STDIN_FILENO, &delay, NULL) == -1) {
                    fprintf(stderr, "Invalid command. See HELP for usage\n");
                    continue;
                }

                if (delay > 0) {
                    printf("Waiting...\n");
                    kvs_wait(delay);
                }
                break;

            case CMD_BACKUP:
                if (kvs_backup()) {
                    fprintf(stderr, "Failed to perform backup.\n");
                }
                break;

            case CMD_INVALID:
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                break;

            case CMD_HELP:
                printf(
                    "Available commands:\n"
                    "  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n"
                    "  HELP\n"
                );
                break;

            case CMD_EMPTY:
                break;

            case EOC:
                kvs_terminate();
                return 0;
        }
    }
}

// Função para processar todos os arquivos .job em um diretório
void process_directory(const char *job_directory) {
    DIR *dir = opendir(job_directory);
    if (!dir) {
        perror("Failed to open job directory");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job")) { // Processar apenas arquivos com extensão .job
            char input_path[MAX_JOB_FILE_NAME_SIZE];
            char output_path[MAX_JOB_FILE_NAME_SIZE];

            int input_len = snprintf(input_path, sizeof(input_path), "%s/%s", job_directory, entry->d_name);
            if (input_len < 0 || input_len >= (int)sizeof(input_path)) {
                fprintf(stderr, "Input path too long: %s/%s\n", job_directory, entry->d_name);
                continue; // Pular para o próximo arquivo
            }

            int output_len = snprintf(output_path, sizeof(output_path), "%s/%s.out", job_directory, entry->d_name);
            if (output_len < 0 || output_len >= (int)sizeof(output_path)) {
                fprintf(stderr, "Output path too long: %s/%s.out\n", job_directory, entry->d_name);
                continue; // Pular para o próximo arquivo
            }

            printf("Processing job file: %s\n", input_path);
            process_job_file(input_path, output_path);
        }
    }

    closedir(dir);
}


