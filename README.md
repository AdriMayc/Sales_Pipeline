
# Sales Pipeline

Este projeto implementa um pipeline completo de ETL para dados de vendas, utilizando Airflow, PostgreSQL, LocalStack (simulando AWS), e Streamlit para visualização. O objetivo é demonstrar habilidades práticas em Data Engineering, desde a ingestão, transformação, armazenamento, até a visualização dos dados.

---

## Estrutura do Projeto

- **ETL com Airflow:** Automatiza a extração dos dados do bucket S3 (LocalStack), validação, carregamento no banco, exportação para parquet e carga da tabela fato.
- **PostgreSQL:** Banco relacional para armazenar dados brutos (tabela `sales`) e dados transformados (tabela `fact_sales`).
- **LocalStack:** Emulação local dos serviços AWS (S3), para desenvolvimento e testes locais.
- **Streamlit:** Dashboard para visualização dos dados e análises rápidas.

---

## Como Executar

1. **Configurar ambiente:**

- Ajuste as variáveis de ambiente e o arquivo `.env` com a chave Fernet para o Airflow.
- Tenha Docker e Docker Compose instalados.

2. **Subir o ambiente:**

```bash
docker-compose up -d
```

3. **Aguardar o pipeline:**

- O Airflow vai aguardar o arquivo CSV no bucket `sales-raw` do LocalStack.
- Quando disponível, executa as tarefas em sequência para popular o banco e gerar arquivos parquet.

4. **Acessar Streamlit:**

- O app Streamlit estará disponível na porta 8501 (`http://localhost:8501`).
- Visualize as tabelas brutas, dados fato e gráficos analíticos.

---

## Visualizações

### 1. Tabela: Dados Brutos (sales_data)

Aqui são exibidos os dados originais extraídos do CSV, com as colunas completas.

<img width="1919" height="831" alt="img1" src="https://github.com/user-attachments/assets/9b2a7b35-64de-4fe0-9756-ad5374e1bb8d" />

### 2. Tabela: Dados Fato (fact_sales)

Dados processados, incluindo o cálculo do total_price.

<img width="1587" height="535" alt="img 2" src="https://github.com/user-attachments/assets/19413b29-611e-417e-9963-7001b2c98d7c" />

### 3. Gráfico: Análise Rápida

Visualização gráfica que destaca a distribuição de vendas por país e outras métricas.

<img width="1557" height="814" alt="img3" src="https://github.com/user-attachments/assets/dd258f3b-7ce4-4184-8bd8-68c9a6187f4b" />

---

## Considerações Finais

Este projeto serve para demonstrar conhecimentos em:

- Engenharia de dados com Airflow e PostgreSQL
- Integração com AWS (simulada com LocalStack)
- Manipulação e validação de dados com Pandas
- Visualização interativa com Streamlit

Para rodar localmente, assegure-se de seguir os passos no `.env` e o docker-compose corretamente configurado.

---

### Desenvolvido por Adriano — Projeto Sales Pipeline | Streamlit & Python
