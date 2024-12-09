#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <sys/stat.h>
#include "job_processor.h"
#include "constants.h"
#include "parser.h"
#include "operations.h"

// Função para processar todos os arquivos .job em um diretório
void process_directory(const char *job_directory) {
    DIR *dir = opendir(job_directory);
    if (!dir) {
        perror("Failed to open job directory");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        // Ignorar entradas que não terminam em .job
        if (!strstr(entry->d_name, ".job")) continue;

        // Ignorar arquivos que terminam em .out (evitar processamento de resultados gerados)
        if (strstr(entry->d_name, ".out")) continue;

        char input_path[MAX_JOB_FILE_NAME_SIZE];
        char output_path[MAX_JOB_FILE_NAME_SIZE];

        // Construir caminhos para entrada e saída
        int input_len = snprintf(input_path, sizeof(input_path), "%s/%s", job_directory, entry->d_name);
        if (input_len < 0 || input_len >= (int)sizeof(input_path)) {
            fprintf(stderr, "Input path too long: %s/%s\n", job_directory, entry->d_name);
            continue;
        }

        int output_len = snprintf(output_path, sizeof(output_path), "%s/%s.out", job_directory, entry->d_name);
        if (output_len < 0 || output_len >= (int)sizeof(output_path)) {
            fprintf(stderr, "Output path too long: %s/%s.out\n", job_directory, entry->d_name);
            continue;
        }

        printf("Processing job file: %s\n", input_path);
        process_job_file(input_path, output_path);
    }

    closedir(dir);
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <directory or file>\n", argv[0]);
        return EXIT_FAILURE;
    }

    // Inicializar o sistema KVS
    if (kvs_init() != 0) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return EXIT_FAILURE;
    }

    struct stat path_stat;
    if (stat(argv[1], &path_stat) != 0) {
        perror("Error checking path");
        kvs_terminate();
        return EXIT_FAILURE;
    }

    if (S_ISDIR(path_stat.st_mode)) {
        // Processar todos os arquivos .job no diretório
        process_directory(argv[1]);
    } else if (S_ISREG(path_stat.st_mode)) {
        // Processar um único arquivo .job
        char output_path[MAX_JOB_FILE_NAME_SIZE];
        snprintf(output_path, sizeof(output_path), "%s.out", argv[1]);

        printf("Processing single job file: %s\n", argv[1]);
        process_job_file(argv[1], output_path);
    } else {
        fprintf(stderr, "Error: Path is neither a directory nor a file.\n");
        kvs_terminate();
        return EXIT_FAILURE;
    }

    // Finalizar o sistema KVS
    if (kvs_terminate() != 0) {
        fprintf(stderr, "Failed to terminate KVS\n");
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
