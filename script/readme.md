# Upload de Arquivos CSV

Script Python para fazer upload de múltiplos arquivos CSV para o servidor.

## 📋 Pré-requisitos

- Python 3.6 ou superior instalado
- Biblioteca `requests` instalada
- Conexão com a internet
- Arquivos CSV para enviar

## 🔧 Instalação

### Passo 1: Verificar se o Python está instalado

Abra o terminal (ou Prompt de Comando no Windows) e digite:

```bash
python --version
```

Ou no Linux/Mac:

```bash
python3 --version
```

Se não tiver Python instalado, baixe em [python.org](https://www.python.org/downloads/)

### Passo 2: Instalar a biblioteca requests

No terminal, execute:

```bash
pip install requests
```

Ou no Linux/Mac:

```bash
pip3 install requests
```

## 🚀 Como Usar

### 1. Prepare seus arquivos CSV

Coloque todos os arquivos CSV que deseja enviar em uma pasta.

### 2. Execute o script

**No Windows:**
- Abra o Prompt de Comando (CMD)
- Navegue até a pasta onde está o script:
  ```
  cd C:\caminho\para\pasta\do\script
  ```
- Execute:
  ```
  python insert_file_on_s3.py
  ```

**No Linux/Mac:**
- Abra o Terminal
- Navegue até a pasta do script:
  ```
  cd /caminho/para/pasta/do/script
  ```
- Execute:
  ```
  python3 insert_file_on_s3.py
  ```

### 3. Informe o caminho da pasta

Quando o script pedir, digite o caminho completo da pasta contendo seus arquivos CSV.

**Exemplos de caminhos válidos:**

**Windows:**
```
C:\Users\SeuNome\Documents\MeusDados
```
ou
```
C:\dados\csvs
```

**Linux/Mac:**
```
/home/seunome/documentos/dados
```
ou
```
/Users/seunome/Documents/dados
```

**Caminho relativo (pasta próxima ao script):**
```
./dados
```
ou simplesmente
```
dados
```

### 4. Aguarde o resultado

O script irá:
- Verificar se a pasta existe
- Contar quantos arquivos CSV foram encontrados
- Enviar todos os arquivos para o servidor
- Mostrar se o upload foi bem-sucedido ou se houve erro

## ✅ Exemplo de Uso

```
$ python3 upload_csv.py
Digite o caminho da pasta contendo os arquivos CSV: ./meus_csvs
Encontrados 5 arquivo(s) CSV. Enviando...
Upload realizado com sucesso!
{'message': 'Arquivos recebidos com sucesso', 'total': 5}
```

## ⚠️ Possíveis Erros e Soluções

### "Erro: A pasta 'xxx' não existe."
**Causa:** O caminho digitado está incorreto ou a pasta não existe.

**Solução:** 
- Verifique se digitou o caminho corretamente
- Certifique-se de que a pasta realmente existe
- Use aspas se o caminho tiver espaços: `"C:\Meus Documentos\dados"`

### "Erro: 'xxx' não é uma pasta válida."
**Causa:** Você digitou o caminho de um arquivo ao invés de uma pasta.

**Solução:** Digite o caminho da pasta que contém os CSVs, não o caminho de um arquivo CSV individual.

### "Nenhum arquivo CSV encontrado na pasta."
**Causa:** A pasta não contém arquivos com extensão `.csv`

**Solução:**
- Verifique se os arquivos têm a extensão `.csv` (não `.txt`, `.xlsx`, etc.)
- Confirme se está apontando para a pasta correta

### "ModuleNotFoundError: No module named 'requests'"
**Causa:** A biblioteca `requests` não está instalada.

**Solução:** Execute `pip install requests`

### Erro de conexão
**Causa:** Servidor fora do ar ou sem conexão com a internet.

**Solução:**
- Verifique sua conexão com a internet
- Confirme se o servidor está funcionando
- Entre em contato com o administrador do sistema

## 📝 Observações Importantes

- Apenas arquivos com extensão `.csv` serão enviados
- O script envia TODOS os CSVs da pasta de uma só vez
- Os arquivos não são modificados ou excluídos após o envio
- Certifique-se de ter permissão de leitura nos arquivos