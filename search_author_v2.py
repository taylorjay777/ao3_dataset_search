import os
import json
import zstandard as zstd
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# === CONFIGURATION ===
target_author = input("Type Author Name Here:")  # Change as needed
input_folder = input("Paste the file directory where dataset is stored here:")  # Current directory
output_file = input_folder + "/fic_list.jsonl"
max_workers = 6  # Adjust based on how powerful your Computer is (1 Worker per core recommended)

# === CLEANING FUNCTION ===
def clean_author(author_field):
    author_field = author_field.lower().strip()
    author_field = re.sub(r"^by\s+", "", author_field)
    return author_field

# === WORKER FUNCTION: Process one file ===
def process_file(filename, author):
    matches = []
    full_path = os.path.join(input_folder, filename)
    try:
        with open(full_path, "rb") as fh:
            dctx = zstd.ZstdDecompressor()
            stream_reader = dctx.stream_reader(fh)
            reader = io.TextIOWrapper(stream_reader, encoding="utf-8")

            for line in reader:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    metadata = data.get("metadata", {})
                    author_field = metadata.get("author", "")

                    author_name = clean_author(author_field)

                    if clean_author(author) == author_name:
                        matches.append(data)
                except Exception:
                    continue  # skip broken/bad lines
    except Exception as e:
        print(f"Error processing {filename}: {e}")

    return matches

# === MAIN FUNCTION ===
def search_zst_files_by_author_parallel():
    all_files = [f for f in os.listdir(input_folder) if f.endswith(".zst")]
    total_matches = 0

    with open(output_file, "w", encoding="utf-8") as out_f:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(process_file, filename, target_author): filename for filename in all_files}

            for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Overall progress", unit="file"):
                matches = future.result()
                for match in matches:
                    out_f.write(json.dumps(match) + "\n")
                    total_matches += 1
                    print(f"📚 Total matches so far: {total_matches}", end="\r")

    print(f"\n🎉 Done. Found {total_matches} matching entries written to {output_file}.")

# === RUN ===
if __name__ == "__main__":
    search_zst_files_by_author_parallel()
