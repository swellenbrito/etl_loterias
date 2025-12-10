#!/usr/bin/env python3
"""
ETL Loteria (simples, claro e completo)

- Lê JSON (lista de concursos ou objeto com lista interna)
- Transforma e limpa:
  * datas (aceita dd/mm/yyyy e yyyy-mm-dd -> armazena ISO yyyy-mm-dd)
  * dezenas (converte strings "02" -> int 2)
  * uf: normaliza; valores inválidos viram NULL
  * remove/normaliza acentos e espaços em textos
  * conta e reporta valores nulos
- Cria SQLite com tabelas:
  * estados(uf, nome_estado, regiao)
  * sorteios(concurso, loteria, data, local, valores numéricos...)
  * dezenas(concurso, posicao, numero)
  * premiacoes(concurso, faixa, descricao, ganhadores, valor)
  * ganhadores(concurso, municipio, uf, ganhadores)
- Modo --preview para ver contagens, primeiros 5 registros e relatório de nulos
- Simples: usa apenas stdlib (sqlite3, unicodedata)
"""

import argparse
import json
import sqlite3
from datetime import datetime
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

# -------------------------
# Dicionário de Estados com Região
# -------------------------
# Fonte: regiões brasileiras (simplificada)
ESTADOS_INFO = {
    "AC": ("Acre", "Norte"),
    "AL": ("Alagoas", "Nordeste"),
    "AP": ("Amapa", "Norte"),
    "AM": ("Amazonas", "Norte"),
    "BA": ("Bahia", "Nordeste"),
    "CE": ("Ceara", "Nordeste"),
    "DF": ("Distrito Federal", "Centro-Oeste"),
    "ES": ("Espirito Santo", "Sudeste"),
    "GO": ("Goias", "Centro-Oeste"),
    "MA": ("Maranhao", "Nordeste"),
    "MT": ("Mato Grosso", "Centro-Oeste"),
    "MS": ("Mato Grosso do Sul", "Centro-Oeste"),
    "MG": ("Minas Gerais", "Sudeste"),
    "PA": ("Para", "Norte"),
    "PB": ("Paraiba", "Nordeste"),
    "PR": ("Parana", "Sul"),
    "PE": ("Pernambuco", "Nordeste"),
    "PI": ("Piaui", "Nordeste"),
    "RJ": ("Rio de Janeiro", "Sudeste"),
    "RN": ("Rio Grande do Norte", "Nordeste"),
    "RS": ("Rio Grande do Sul", "Sul"),
    "RO": ("Rondonia", "Norte"),
    "RR": ("Roraima", "Norte"),
    "SC": ("Santa Catarina", "Sul"),
    "SP": ("Sao Paulo", "Sudeste"),
    "SE": ("Sergipe", "Nordeste"),
    "TO": ("Tocantins", "Norte"),
}

VALID_UFS = set(ESTADOS_INFO.keys())

# -------------------------
# Helpers: limpeza e parsing
# -------------------------
def clean_text(s: Optional[str]) -> Optional[str]:
    """Remove acentos, strip e padroniza maiúsculas/minúsculas (Title case)."""
    if s is None:
        return None
    s = str(s).strip()
    if s == "":
        return None
    # remover acentos
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # padronizar espaços e capitalização simples
    return " ".join(s.split()).title()

def parse_date_any(value: Optional[str]) -> Optional[str]:
    """Aceita dd/mm/YYYY ou YYYY-mm-dd. Retorna ISO YYYY-mm-dd ou None."""
    if not value:
        return None
    v = str(value).strip()
    # já ISO?
    if "-" in v:
        try:
            dt = datetime.strptime(v, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            # tenta aceitar se vier com tempo
            try:
                dt = datetime.fromisoformat(v)
                return dt.date().isoformat()
            except Exception:
                return None
    # formato BR dd/mm/YYYY
    if "/" in v:
        try:
            dt = datetime.strptime(v, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None
    return None

def normalize_dezenas(nums: Optional[List[Any]]) -> List[int]:
    """Converte lista de strings/números para lista de ints; ignora inválidos."""
    if not nums:
        return []
    out = []
    for x in nums:
        if x is None:
            continue
        sx = str(x).strip()
        # extrai dígitos
        digits = "".join(ch for ch in sx if ch.isdigit())
        if digits == "":
            continue
        try:
            out.append(int(digits))
        except Exception:
            continue
    return out

def normalize_uf(uf: Optional[str]) -> Optional[str]:
    """Padroniza UF: retorna 'SP' etc se válido, caso contrário retorna None."""
    if not uf:
        return None
    s = str(uf).strip().upper()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace(".", "").replace(",", "").strip()
    # valores comuns que indicam vazio
    if s in ("", "--", "NA", "N/A", "NULL", "NONE", "0"):
        return None
    # se tiver espaço, tentar última palavra (ex: "Boa Vista RR" -> RR)
    if " " in s:
        parts = [p for p in s.split(" ") if p]
        cand = parts[-1]
        if cand in VALID_UFS:
            return cand
    # se tiver 2 letras válidas
    if len(s) == 2 and s in VALID_UFS:
        return s
    # se tiver mais que 2 e conteúdo válido contido
    for cand in VALID_UFS:
        if cand in s:
            return cand
    return None

# -------------------------
# Extract
# -------------------------
def extract(path: str):
    """Lê JSON do caminho; retorna lista de registros (cada registro um dict)."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # procurar a primeira lista interna plausível
        for v in data.values():
            if isinstance(v, list):
                return v
        # caso não haja lista, retornar o objeto único
        return [data]
    # retorno padrão
    return []

# -------------------------
# Transform
# -------------------------
def transform(records: List[Dict[str, Any]]):
    """
    Transforma registros brutos em conjuntos prontos para inserção:
    - sorteios: lista de dicts
    - dezenas: lista de dicts (posicional)
    - premiacoes: lista de dicts
    - ganhadores: lista de dicts
    - estados_set: set de UFs válidos (para popular tabela estados)
    Também calcula contagem de nulos por coluna para relatório.
    """
    sorteios = []
    dezenas = []
    premiacoes = []
    ganhadores = []
    estados_set = set()

    # contadores de nulos por campo (apenas para reporte)
    null_counts = {
        "concurso_missing": 0,
        "data_missing": 0,
        "local_missing": 0,
        "dezenas_missing": 0,
        "uf_missing": 0,
    }

    for rec in records:
        # identificar concurso (campo esperado 'concurso')
        concurso = rec.get("concurso")
        if concurso is None:
            null_counts["concurso_missing"] += 1
            # ignorar registro sem concurso
            continue

        # data
        data_iso = parse_date_any(rec.get("data"))
        if not data_iso:
            null_counts["data_missing"] += 1

        # local (texto)
        local = clean_text(rec.get("local"))
        if local is None:
            null_counts["local_missing"] += 1

        # campos numéricos opcionais (vamos incluir alguns numéricos relevantes)
        def to_float_safe(k):
            v = rec.get(k)
            try:
                if v is None:
                    return None
                return float(v)
            except Exception:
                return None

        valorArrecadado = to_float_safe("valorArrecadado")
        valorAcumuladoConcurso_0_5 = to_float_safe("valorAcumuladoConcurso_0_5")
        valorAcumuladoConcursoEspecial = to_float_safe("valorAcumuladoConcursoEspecial")
        valorAcumuladoProximoConcurso = to_float_safe("valorAcumuladoProximoConcurso")
        valorEstimadoProximoConcurso = to_float_safe("valorEstimadoProximoConcurso")

        sorteios.append({
            "concurso": int(concurso),
            "loteria": clean_text(rec.get("loteria")),
            "data": data_iso,
            "local": local,
            "valorArrecadado": valorArrecadado,
            "valorAcumuladoConcurso_0_5": valorAcumuladoConcurso_0_5,
            "valorAcumuladoConcursoEspecial": valorAcumuladoConcursoEspecial,
            "valorAcumuladoProximoConcurso": valorAcumuladoProximoConcurso,
            "valorEstimadoProximoConcurso": valorEstimadoProximoConcurso,
        })

        # dezenas em ordem do sorteio (se existir)
        dez_ordem = rec.get("dezenasOrdemSorteio") or rec.get("dezenas") or []
        nums = normalize_dezenas(dez_ordem)
        if not nums:
            null_counts["dezenas_missing"] += 1
        for pos, num in enumerate(nums, start=1):
            dezenas.append({
                "concurso": int(concurso),
                "posicao": pos,
                "numero": int(num)
            })

        # premiacoes (lista)
        for p in rec.get("premiacoes", []) or []:
            premiacoes.append({
                "concurso": int(concurso),
                "faixa": p.get("faixa"),
                "descricao": clean_text(p.get("descricao")),
                "ganhadores": p.get("ganhadores"),
                "valorPremio": (lambda x: float(x) if x is not None else None)(p.get("valorPremio"))
            })

        # localGanhadores -> ganhadores por cidade/uf
        for g in rec.get("localGanhadores", []) or []:
            uf_raw = g.get("uf")
            uf = normalize_uf(uf_raw)
            if uf is None:
                null_counts["uf_missing"] += 1
            else:
                estados_set.add(uf)
            ganhadores.append({
                "concurso": int(concurso),
                "municipio": clean_text(g.get("municipio")),
                "uf": uf,
                "ganhadores": g.get("ganhadores")
            })

    return sorteios, dezenas, premiacoes, ganhadores, estados_set, null_counts

# -------------------------
# Load (SQLite)
# -------------------------
def load_sqlite(sqlite_path: str,
                sorteios: List[Dict[str, Any]],
                dezenas: List[Dict[str, Any]],
                premiacoes: List[Dict[str, Any]],
                ganhadores: List[Dict[str, Any]],
                estados_set: set):
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()

    # criar tabelas
    c.execute("""
    CREATE TABLE IF NOT EXISTS estados (
        uf TEXT PRIMARY KEY,
        nome_estado TEXT,
        regiao TEXT
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS sorteios (
        concurso INTEGER PRIMARY KEY,
        loteria TEXT,
        data TEXT,
        local TEXT,
        valorArrecadado REAL,
        valorAcumuladoConcurso_0_5 REAL,
        valorAcumuladoConcursoEspecial REAL,
        valorAcumuladoProximoConcurso REAL,
        valorEstimadoProximoConcurso REAL
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS dezenas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concurso INTEGER,
        posicao INTEGER,
        numero INTEGER
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS premiacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concurso INTEGER,
        faixa INTEGER,
        descricao TEXT,
        ganhadores INTEGER,
        valorPremio REAL
    );
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS ganhadores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concurso INTEGER,
        municipio TEXT,
        uf TEXT,
        ganhadores INTEGER
    );
    """)

    # popular estados (com nome e regiao) — incluir todos os ESTADOS_INFO se quiser,
    # mas vamos inserir apenas os presentes no conjunto para ser conciso.
    for uf in sorted(estados_set):
        info = ESTADOS_INFO.get(uf)
        if info:
            nome, regiao = info
        else:
            nome, regiao = (None, None)
        c.execute("INSERT OR IGNORE INTO estados (uf, nome_estado, regiao) VALUES (?, ?, ?)",
                  (uf, nome, regiao))

    # inserir sorteios (usamos INSERT OR REPLACE para atualizar caso já exista)
    for s in sorteios:
        c.execute("""
        INSERT OR REPLACE INTO sorteios
        (concurso, loteria, data, local,
         valorArrecadado, valorAcumuladoConcurso_0_5,
         valorAcumuladoConcursoEspecial, valorAcumuladoProximoConcurso,
         valorEstimadoProximoConcurso)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            s["concurso"], s.get("loteria"), s.get("data"), s.get("local"),
            s.get("valorArrecadado"), s.get("valorAcumuladoConcurso_0_5"),
            s.get("valorAcumuladoConcursoEspecial"), s.get("valorAcumuladoProximoConcurso"),
            s.get("valorEstimadoProximoConcurso")
        ))

    # dezenas (executemany em blocos)
    if dezenas:
        dezenas_rows = [(d["concurso"], d["posicao"], d["numero"]) for d in dezenas]
        c.executemany("INSERT INTO dezenas (concurso, posicao, numero) VALUES (?, ?, ?)", dezenas_rows)

    # premiacoes
    if premiacoes:
        prem_rows = [(p["concurso"], p.get("faixa"), p.get("descricao"), p.get("ganhadores"), p.get("valorPremio")) for p in premiacoes]
        c.executemany("INSERT INTO premiacoes (concurso, faixa, descricao, ganhadores, valorPremio) VALUES (?, ?, ?, ?, ?)", prem_rows)

    # ganhadores
    if ganhadores:
        ganh_rows = [(g["concurso"], g.get("municipio"), g.get("uf"), g.get("ganhadores")) for g in ganhadores]
        c.executemany("INSERT INTO ganhadores (concurso, municipio, uf, ganhadores) VALUES (?, ?, ?, ?)", ganh_rows)

    conn.commit()
    conn.close()

# -------------------------
# Relatório simples de nulos
# -------------------------
def report_nulls(null_counts: Dict[str, int], total_input: int):
    print("\n--- Relatório de Nulos / Registros Ignorados ---")
    print(f"Registros totais no arquivo: {total_input}")
    print(f"Registros sem 'concurso' (ignorados): {null_counts.get('concurso_missing', 0)}")
    print(f"Registros com data inválida/ausente: {null_counts.get('data_missing', 0)}")
    print(f"Registros com local ausente: {null_counts.get('local_missing', 0)}")
    print(f"Registros sem dezenas: {null_counts.get('dezenas_missing', 0)}")
    print(f"Quantidade de UFs inválidas/ausentes encontradas: {null_counts.get('uf_missing', 0)}")
    print("--- fim relatório ---\n")

# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser(description="ETL simples para Loteria -> SQLite")
    parser.add_argument("--input", "-i", required=True, help="arquivo JSON de entrada")
    parser.add_argument("--output", "-o", required=False, help="arquivo SQLite de saída")
    parser.add_argument("--preview", action="store_true", help="apenas mostra resumo e sai")
    args = parser.parse_args()

    records = extract(args.input)
    sorteios, dezenas, premiacoes, ganhadores, estados_set, null_counts = transform(records)

    # Preview: mostrar contagens, primeiros 5 e relatório de nulos
    if args.preview:
        print("\n=== PREVIEW ===")
        print(f"Registros originais no arquivo: {len(records)}")
        print(f"Sorteios válidos extraídos: {len(sorteios)}")
        print(f"Dezenas extraídas: {len(dezenas)}")
        print(f"Premiações extraídas: {len(premiacoes)}")
        print(f"Ganhadores extraídos: {len(ganhadores)}")
        print(f"UFs válidas detectadas (para popular estados): {sorted(estados_set)}")
        report_nulls(null_counts, len(records))

        print("\n--- Exemplos (até 5) ---")
        print("Sorteios (primeiros 5):")
        for s in sorteios[:5]:
            print(s)
        print("\nDezenas (primeiros 10):")
        for d in dezenas[:10]:
            print(d)
        print("\nPremiacoes (primeiros 5):")
        for p in premiacoes[:5]:
            print(p)
        print("\nGanhadores (primeiros 5):")
        for g in ganhadores[:5]:
            print(g)
        print("\n(Use --output <arquivo.db> para salvar no SQLite)\n")
        return

    # Sem preview: precisa de output
    if not args.output:
        print("Erro: use --output para salvar no banco ou --preview para apenas visualizar.")
        return

    # Escrever SQLite
    load_sqlite(args.output, sorteios, dezenas, premiacoes, ganhadores, estados_set)
    print("ETL concluído com sucesso.")

if __name__ == "__main__":
    main()
