# 📦 Pipeline Medalhão Portwatch — Documentação do Projeto

# Porque o projeto nasceu?

Como profissional formado em Analytics Engineering pela Indicium, minha atuação no dia a dia é focada principalmente nas etapas finais do ciclo de dados — modelagem, padronização e camadas analíticas (SZ → CZ). Apesar disso, sempre tive interesse em dominar também o processo completo, desde a ingestão bruta até a entrega final.

Este projeto nasce justamente com esse propósito:
demonstrar minha capacidade de construir um pipeline ponta a ponta, cobrindo coleta via API, ingestão raw em formato parquet, padronização e validação dos dados, arquitetura do tipo medalhão e orquestração completa via Airflow.

Além de servir como estudos e prática, ele também reforça meu portfólio mostrando que tenho domínio técnico não apenas da camada analítica, mas também de toda a fundação necessária para que ela exista — incluindo ingestão, organização de zonas, qualidade e governança dos dados.

## 🔎 Visão Geral
Este projeto implementa um pipeline de dados baseado na arquitetura Medalhão (Raw → Silver → Gold) utilizando:

- Python
- Airflow (orquestração)
- Formato Parquet nas três camadas
- Consumo de API (ArcGIS/ESRI – Chokepoints)
- Padronização e validação dos dados
- Metadados automáticos em todas as camadas

O objetivo é coletar, tratar e disponibilizar dados históricos sobre tráfego marítimo em pontos críticos globais (“chokepoints”).

# 🧱 Estrutura do Projeto

/dags
└── DAG_Portwatch.py

/scripts
├── Portwatch_rz.py
├── Portwatch_sz.py
└── Portwatch_cz.py

/database
├── RZ/
├── SZ/
└── CZ/

.gitignore
requirements.txt

# 🚀 Fluxo Geral do Pipeline

1. **Camada RZ (Raw Zone)**
   - Conecta na API do ArcGIS.
   - Consulta dados paginados por **ano** e **porto**.
   - Trata tipagem mínima.
   - Remove duplicações.
   - Gera um arquivo Parquet bruto por execução.
   - Gera metadados (.json).
   - Registra log de extração.

2. **Camada SZ (Silver Zone)**
   - Lê *todos os arquivos* da RZ.
   - Consolida tudo em um único dataframe.
   - Padroniza nomes, datas e tipos.
   - Detecta inconsistências de totais.
   - Cria colunas derivadas (ex.: mês-ano).
   - Gera arquivo Parquet tratado.
   - Gera metadados de qualidade.

3. **Camada CZ (Gold Zone)**
   - Lê **apenas o arquivo mais recente da SZ**.
   - Renomeia colunas para termos analíticos.
   - Ajuste final de tipos e formatações.
   - Ordena estrutura final.
   - Exporta o arquivo Parquet Gold.
   - Gera metadados finais.

# ⚙️ Orquestração — Airflow

# 🧪 Qualidade e Confiabilidade

Cada camada gera:
- Arquivo `.parquet`
- Arquivo `.json` com metadados

Os metadados incluem:
- Datas de execução
- Quantidade de registros
- Colunas existentes
- Período de datas
- Inconsistências detectadas
- Arquivos de origem

# 📈 Possíveis Evoluções

- Ingestão incremental na RZ  
- Particionamento do SZ e CZ  
- Padronização com `pyarrow schema`  
- Validação com **Great Expectations**  
- Parametrização via Variables ou Connections do Airflow  
- Inserção em banco analítico (Athena/Snowflake/BigQuery)

# 🏁 Conclusão

O projeto implementa um pipeline robusto, modular e fiel ao padrão medalhão, garantindo:

- rastreabilidade  
- consistência  
- facilidade de manutenção  
- formato analítico final para BI  

Ideal para evoluir em direção a um Data Lake mais amplo ou processos analíticos de maior escala.