import os

def split_file(input_file, output_dir, lines_per_file=1000):
    os.makedirs(output_dir, exist_ok=True)  # создать папку, если не существует

    with open(input_file, 'r', encoding='utf-8') as f:
        file_count = 0
        line_count = 0
        out_path = os.path.join(output_dir, f'split_{file_count}.txt')
        out = open(out_path, 'w', encoding='utf-8')

        for line in f:
            if line_count >= lines_per_file:
                out.close()
                file_count += 1
                out_path = os.path.join(output_dir, f'split_{file_count}.txt')
                out = open(out_path, 'w', encoding='utf-8')
                line_count = 0
            out.write(line)
            line_count += 1

        out.close()

# Пример использования:
split_file("Bitcoin_submissions", "Data", 1000)
