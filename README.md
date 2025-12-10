# 🟩 ETL de Resultados de Loterias (JSON → SQLite)

## 📌 Visão Geral

Este projeto contém um ETL simples e didático que:

1. **Lê** um arquivo JSON com resultados de loterias brasileiras (Mega-Sena, Quina, Lotofácil etc.).
2. **Padroniza** e **limpa** os campos:

   * datas (aceita formatos diferentes)
   * textos com caracteres especiais
   * valores nulos
   * UFs inválidas
3. **Normaliza** em tabelas relacionais:

   * concursos
   * premiações
   * ganhadores por município
   * estados (UF → nome + região)
   
4. **Grava** tudo em um banco SQLite pronto para análise.

O objetivo é ser **simples, legível e fácil de manter**, servindo como material educativo para ETL.

---

## 🗂 Estrutura do Projeto

```
etl_loteria.py       → Script principal do ETL
data/
   dataset.json      → Seu arquivo de entrada (não incluso)
   loteria.db        → Banco SQLite gerado
README.md            → Este arquivo
```

---

## 🧱 Modelo Relacional

### 🟦 concursos

| campo              | tipo                  | descrição |
| ------------------ | --------------------- | --------- |
| id                 | INTEGER (PK)          |           |
| loteria            | TEXT                  |           |
| concurso           | INTEGER               |           |
| data               | TEXT (ISO yyyy-mm-dd) |           |
| local              | TEXT                  |           |
| acumulou           | INTEGER (0/1)         |           |
| observacao         | TEXT                  |           |
| valor_arrecadado   | REAL                  |           |
| prox_concurso      | INTEGER               |           |
| data_prox_concurso | TEXT                  |           |

---

### 🟪 premios

| campo       | tipo                          |
| ----------- | ----------------------------- |
| id          | INTEGER PK                    |
| concurso_id | FK → concursos.id             |
| descricao   | TEXT                          |
| faixa       | INTEGER                       |
| ganhadores  | INTEGER                       |
| valor       | REAL                          |
| dezenas     | TEXT (lista JSON serializada) |

---

### 🟧 ganhadores

| campo       | tipo              |
| ----------- | ----------------- |
| id          | INTEGER PK        |
| concurso_id | FK → concursos.id |
| municipio   | TEXT              |
| uf          | TEXT              |
| ganhadores  | INTEGER           |
| posicao     | INTEGER           |

---

### 🟩 estados

| campo       | tipo    |
| ----------- | ------- |
| uf          | TEXT PK |
| nome_estado | TEXT    |
| regiao      | TEXT    |

UF inválida ou vazia → é convertida para `NULL`.

---

## 🧪 Requisitos

### Python 3.8+

Dependências:

```
sqlalchemy
python-dateutil
```

Instalação:

```bash
pip install sqlalchemy python-dateutil
```

---

## ▶ Como Executar

### 🔍 Modo Preview (não grava no banco)

Mostra contagens, UFs encontradas e registros ignorados:

```bash
python3 etl_loteria.py --input data/dataset.json --preview
```

---

### 💾 Gerar banco SQLite

```bash
python3 etl_loteria.py --input data/dataset.json --output data/loteria.db
```

---

## ♻ Funcionamento do ETL

### 1. **Extract**

* Lê o JSON completo
* Aceita lista ou JSON linha a linha
* Ignora registros sem número de concurso

### 2. **Transform**

* Converte datas para ISO `yyyy-mm-dd`
* Remove caracteres especiais em textos
* Trata valores nulos (`None`, `"N/A"`, `""`)
* Normaliza UF → e descarta valores inválidos (`--`, `XX`, `G` etc.)
* Garante que dezenas sejam armazenadas como JSON
* Cria tabela de estados com nome + região

### 3. **Load**

* Cria automaticamente o SQLite
* Insere concursos, premiações e ganhadores
* Insere a tabela completa de UFs válidas

---

## 📊 Exemplo de Preview

```
=== PREVIEW ===
Concursos: 8174
Premiações: 116757
Ganhadores: 9280
UFs encontradas: PI, BA, SP, RJ, MG, ...
Registros ignorados: 2
```

---

## 📎 Licença

Livre para uso, estudo e modificação.
