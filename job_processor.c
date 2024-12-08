#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"

void process_job_file(const char *input_path, const char *output_path) {
    int input_fd = open(input_path, O_RDONLY);
    if (input_fd < 0) {
        perror("Erro ao abrir arquivo de entrada");
        return;
    }

    int output_fd = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (output_fd < 0) {
        perror("Erro ao criar arquivo de saída");
        close(input_fd);
        return;
    }

    // Redirecionar saída padrão para o arquivo de saída
    int original_stdout = dup(STDOUT_FILENO);
    dup2(output_fd, STDOUT_FILENO);

    // Processar os comandos do arquivo
    while (1) {
        enum Command cmd = get_next(input_fd);
        switch (cmd) {
            case CMD_WRITE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_write(input_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                kvs_write(num_pairs, keys, values);
                break;
            }
            case CMD_READ: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_read_delete(input_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                kvs_read(num_pairs, keys);
                break;
            }
            case CMD_DELETE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_read_delete(input_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                kvs_delete(num_pairs, keys);
                break;
            }
            case CMD_SHOW:
                kvs_show();
                break;
            case CMD_WAIT: {
                unsigned int delay;
                if (parse_wait(input_fd, &delay, NULL) == 0) {
                    kvs_wait(delay);
                }
                break;
            }
            case CMD_BACKUP:
                kvs_backup();
                break;
            case CMD_EMPTY:
                break; // Linha vazia ou comentário
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
            case CMD_INVALID:
                fprintf(stderr, "Unsupported command in batch processing.\n");
                break;
            case EOC:
                close(input_fd);
                close(output_fd);
                dup2(original_stdout, STDOUT_FILENO);
                close(original_stdout);
                return; // Fim do arquivo
            default:
                fprintf(stderr, "Unknown command encountered.\n");
                break;
        }
    }
}
