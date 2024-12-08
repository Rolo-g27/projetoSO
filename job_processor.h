#ifndef JOB_PROCESSOR_H
#define JOB_PROCESSOR_H

/// Processa um arquivo `.job` e gera um arquivo `.out`.
/// @param input_path Caminho para o arquivo `.job`.
/// @param output_path Caminho para o arquivo `.out`.
void process_job_file(const char *input_path, const char *output_path);

#endif // JOB_PROCESSOR_H
