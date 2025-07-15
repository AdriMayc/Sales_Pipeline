import streamlit as st
import pandas as pd

# --- Função para carregar dados ---
@st.cache_data
def load_data(filename):
    return pd.read_parquet(filename)

# --- Configuração da página ---
st.set_page_config(
    page_title="Dashboard Sales Pipeline",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Título e descrição ---
st.title("📊 Sales Pipeline Dashboard")
st.markdown("""
Este dashboard permite explorar os dados de vendas em dois níveis:  
- **Dados Brutos** (`sales_data.parquet`)  
- **Dados Enriquecidos / Fato** (`fact_sales.parquet`) com cálculo de receita total.  
Use a barra lateral para alternar entre os datasets.
""")

# --- Sidebar para escolha do dataset ---
dataset_option = st.sidebar.selectbox(
    "Selecione o dataset para visualizar:",
    ("Dados Brutos (sales_data)", "Dados Fato (fact_sales)")
)

# --- Carregar dados conforme escolha ---
if dataset_option == "Dados Brutos (sales_data)":
    df = load_data("sales_data.parquet")
else:
    df = load_data("fact_sales.parquet")

st.sidebar.markdown(f"**Linhas carregadas:** {len(df):,}")

# --- Mostrar dados com paginação ---
st.subheader(f"Visualização: {dataset_option}")
rows_to_show = st.slider("Quantidade de linhas a exibir:", min_value=5, max_value=100, value=10, step=5)
st.dataframe(df.head(rows_to_show))

# --- KPIs e gráficos para fact_sales ---
if dataset_option == "Dados Fato (fact_sales)":
    st.markdown("---")
    st.subheader("Análise Rápida")

    # KPI Receita Total
    total_revenue = df['total_price'].sum()
    st.metric(label="💰 Receita Total (R$)", value=f"{total_revenue:,.2f}")

    # Receita por país (top 10)
    revenue_by_country = df.groupby("country")["total_price"].sum().sort_values(ascending=False).head(10)
    st.bar_chart(revenue_by_country)

    # Quantidade vendida por produto (top 10)
    quantity_by_product = df.groupby("product_id")["quantity"].sum().sort_values(ascending=False).head(10)
    st.bar_chart(quantity_by_product)

# --- Caso para dados brutos, mostrar algumas estatísticas básicas ---
if dataset_option == "Dados Brutos (sales_data)":
    st.markdown("---")
    st.subheader("Estatísticas básicas dos dados")

    st.write(df.describe(include='all'))

# --- Footer ---
st.markdown("---")

st.markdown(
    """
    <p style='text-align: center; color: gray; font-size: 12px;'>
    Desenvolvido por Adriano — Projeto Sales Pipeline | Streamlit & Python
    </p>
    """,
    unsafe_allow_html=True
)
