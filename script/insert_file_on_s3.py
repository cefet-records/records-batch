import os
import requests

API_URL = "http://18.191.191.71:8000/upload/csv-folder"

# Solicita o caminho da pasta ao usuário
FOLDER_PATH = input("Digite o caminho da pasta contendo os arquivos CSV: ").strip()

# Verifica se a pasta existe
if not os.path.exists(FOLDER_PATH):
    print(f"Erro: A pasta '{FOLDER_PATH}' não existe.")
    exit(1)

if not os.path.isdir(FOLDER_PATH):
    print(f"Erro: '{FOLDER_PATH}' não é uma pasta válida.")
    exit(1)

# Coleta os arquivos CSV
files = []
for filename in os.listdir(FOLDER_PATH):
    if filename.endswith(".csv"):
        filepath = os.path.join(FOLDER_PATH, filename)
        files.append(
            ("files", (filename, open(filepath, "rb"), "text/csv"))
        )

if not files:
    print("Nenhum arquivo CSV encontrado na pasta.")
    exit(0)

print(f"Encontrados {len(files)} arquivo(s) CSV. Enviando...")

# Envia os arquivos
response = requests.post(API_URL, files=files)

# Fecha os arquivos abertos
for _, (_, file_obj, _) in files:
    file_obj.close()

if response.status_code == 200:
    print("Upload realizado com sucesso!")
    print(response.json())
else:
    print("Erro no upload:")
    print(response.status_code, response.text)
