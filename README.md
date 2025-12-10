# etl_loterias
ETL Python para resultados de loterias - JSON → SQLite
# Data Engineer Pleno

Este desafio tem como objetivo avaliar as habilidades técnicas e de raciocínio da pessoa candidata à vaga de Data Engineer Pleno na **Pier**. Queremos conhecer como você pensa, estrutura e implementa soluções de dados, da extração à disponibilização para análise.

## Objetivo

Você receberá um arquivo **JSON** com dados brutos. Seu desafio é construir um pipeline de dados completo (ETL) que realize:

1. **Extração:** Ler os dados do arquivo JSON;
2. **Transformação:** Limpar, padronizar e transformar os dados conforme necessário. Isso pode incluir:
    - Tratamento de valores ausentes, duplicados ou inconsistentes;
    - Conversão e padronização de tipos de dados;
    - Normalização de valores e criação de colunas derivadas;
    - Qualquer outra transformação que melhore a qualidade e a consistência dos dados.
3. **Carregamento:** Gravar os dados transformados em um **banco de dados relacional SQLite**, pronto para consultas analíticas;
4. **Documentação:** Explicar claramente o processo de ETL, incluindo as decisões e justificativas para as transformações realizadas.

## Sobre os Dados

O arquivo JSON contém uma série histórica de resultados da Loteria Federal. Cada registro inclui campos como data do sorteio, números sorteados e outras informações relevantes.

A modelagem do banco de dados é **livre**. Você pode definir a estrutura e os relacionamentos como achar mais adequado. Esse ponto será avaliado considerando clareza, normalização e facilidade de análise.

É importante destacar que a vaga é para um engenheiro de dados. Portanto, vale pensar tanto na modelagem das camadas de dados, quanto modelagem das entidades e atributos.

## Entregáveis

Sua entrega deverá ser feita em um **repositório no GitHub**, contendo:

- O **código-fonte** completo do pipeline de dados (ETL);
- Um arquivo `README.md` com:
    - Instruções claras sobre como executar o pipeline;
    - Dependências e requisitos;
    - Breve explicação sobre o design e as decisões do projeto;
- A **documentação** do processo de ETL (pode estar no README ou em um arquivo separado);
- O arquivo **SQLite** com os dados já transformados e modelados.

## Requisitos Técnicos

- Linguagem obrigatória: **Python**;
- Banco de dados: **SQLite**;
- Você tem liberdade total para escolher as bibliotecas, frameworks ou ferramentas auxiliares que julgar mais adequados;
- O código deve ser legível, organizado e reprodutível.

## Critérios de Avaliação

Durante a análise do seu desafio, observaremos principalmente:

- Qualidade e organização do código;
- Clareza e coerência da modelagem de dados;
- Tratamento e transformação adequados dos dados;
- Documentação e explicação do raciocínio técnico;
- Capacidade de comunicação e justificativa das escolhas.

O desafio é aberto, e esperamos ver soluções criativas e bem fundamentadas. Use as melhores abordagens que você conhece e sinta-se à vontade para inovar.

## Apresentação

Você será convidada(o) para uma entrevista técnica. Nela, poderá apresentar seu trabalho, explicar suas decisões e responder perguntas sobre a solução proposta.

Você tem liberdade para apresentar da forma que achar melhor, o uso de slides é totalmente opcional. Nosso objetivo é entender seu raciocínio, clareza técnica e capacidade de comunicação.
