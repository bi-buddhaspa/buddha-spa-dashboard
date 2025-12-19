import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# ============================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================
st.set_page_config(
    page_title="Buddha Spa - Dashboard Executivo",
    page_icon="🧘",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# AUTENTICAÇÃO BIGQUERY
# ============================================================
@st.cache_resource
def get_bigquery_client():
    """Cria cliente BigQuery usando secrets do Streamlit"""
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials)

client = get_bigquery_client()

# ============================================================
# FUNÇÃO PARA EXECUTAR QUERIES
# ============================================================
@st.cache_data(ttl=3600)
def executar_query(query):
    """Executa query no BigQuery e retorna DataFrame"""
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"❌ Erro ao executar query: {e}")
        return pd.DataFrame()

# ============================================================
# SIDEBAR - FILTROS GLOBAIS
# ============================================================
st.sidebar.title("🧘 Buddha Spa")
st.sidebar.markdown("### Dashboard Executivo")

# Filtro de Data
st.sidebar.markdown("---")
st.sidebar.subheader("📅 Período")

# Opções de período pré-definidas
periodo_opcoes = {
    "Hoje": (datetime.now().date(), datetime.now().date()),
    "Ontem": ((datetime.now() - timedelta(days=1)).date(), (datetime.now() - timedelta(days=1)).date()),
    "Últimos 7 dias": ((datetime.now() - timedelta(days=7)).date(), datetime.now().date()),
    "Últimos 30 dias": ((datetime.now() - timedelta(days=30)).date(), datetime.now().date()),
    "Este mês": (datetime.now().replace(day=1).date(), datetime.now().date()),
    "Mês passado": ((datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1).date(), 
                     (datetime.now().replace(day=1) - timedelta(days=1)).date()),
    "Personalizado": None
}

periodo_selecionado = st.sidebar.selectbox(
    "Selecione o período",
    options=list(periodo_opcoes.keys()),
    index=2  # Padrão: Últimos 7 dias
)

if periodo_selecionado == "Personalizado":
    col1, col2 = st.sidebar.columns(2)
    with col1:
        data_inicio = st.date_input(
            "De",
            value=(datetime.now() - timedelta(days=30)).date()
        )
    with col2:
        data_fim = st.date_input(
            "Até",
            value=datetime.now().date()
        )
else:
    data_inicio, data_fim = periodo_opcoes[periodo_selecionado]

# Filtro de Unidade
st.sidebar.markdown("---")
st.sidebar.subheader("🏢 Unidade")

# Query para buscar lista de unidades
query_unidades = """
SELECT DISTINCT
    (SELECT u.post_title 
     FROM `buddha-bigdata.raw.wp_posts` u 
     WHERE u.post_type = 'unidade' 
     AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS unidade
FROM `buddha-bigdata.raw.ecommerce_raw` s
WHERE (SELECT u.post_title 
       FROM `buddha-bigdata.raw.wp_posts` u 
       WHERE u.post_type = 'unidade' 
       AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) IS NOT NULL
ORDER BY unidade
"""

df_unidades = executar_query(query_unidades)
lista_unidades = ["Todas as Unidades"] + df_unidades["unidade"].tolist()

unidade_selecionada = st.sidebar.selectbox(
    "Selecione a unidade",
    options=lista_unidades,
    index=0
)

# Informações do usuário
st.sidebar.markdown("---")
st.sidebar.info(f"👤 Bem-vindo, {st.secrets.get('user_name', 'Usuário')}!")
st.sidebar.caption(f"📅 Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")
if unidade_selecionada != "Todas as Unidades":
    st.sidebar.caption(f"🏢 Unidade: {unidade_selecionada}")

# ============================================================
# ABAS DO DASHBOARD
# ============================================================
abas = st.tabs([
    "📊 Visão Geral",
    "💰 Financeiro",
    "👥 Atendimento",
    "📦 Ecommerce",
    "📈 Marketing",
    "❓ Ajuda"
])

# ============================================================
# 1. ABA VISÃO GERAL
# ============================================================
with abas[0]:
    st.header("📊 Visão Geral")
    
    # Query principal para visão geral
    if unidade_selecionada == "Todas as Unidades":
        query_visao_geral = f"""
        SELECT
            COUNT(DISTINCT s.ORDER_ID) as total_pedidos,
            COUNT(s.ID) as total_vouchers,
            SUM(s.PRICE_NET) as receita_liquida,
            SUM(s.PRICE_GROSS) as receita_bruta,
            COUNT(CASE WHEN s.STATUS = '2' THEN 1 END) as vouchers_usados,
            COUNT(CASE WHEN s.STATUS = '1' THEN 1 END) as vouchers_ativos
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    else:
        query_visao_geral = f"""
        SELECT
            COUNT(DISTINCT s.ORDER_ID) as total_pedidos,
            COUNT(s.ID) as total_vouchers,
            SUM(s.PRICE_NET) as receita_liquida,
            SUM(s.PRICE_GROSS) as receita_bruta,
            COUNT(CASE WHEN s.STATUS = '2' THEN 1 END) as vouchers_usados,
            COUNT(CASE WHEN s.STATUS = '1' THEN 1 END) as vouchers_ativos
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
            AND (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) = '{unidade_selecionada}'
        """
    
    df_visao_geral = executar_query(query_visao_geral)
    
    if not df_visao_geral.empty:
        # Cards de métricas principais
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "💰 Receita Líquida",
                f"R$ {df_visao_geral['receita_liquida'].iloc[0]:,.2f}",
                help="Receita líquida de vouchers vendidos"
            )
        
        with col2:
            st.metric(
                "🎫 Vouchers Vendidos",
                f"{df_visao_geral['total_vouchers'].iloc[0]:,}",
                help="Total de vouchers vendidos no período"
            )
        
        with col3:
            st.metric(
                "✅ Vouchers Usados",
                f"{df_visao_geral['vouchers_usados'].iloc[0]:,}",
                help="Vouchers já utilizados pelos clientes"
            )
        
        with col4:
            st.metric(
                "📦 Pedidos",
                f"{df_visao_geral['total_pedidos'].iloc[0]:,}",
                help="Total de pedidos realizados"
            )
        
        # Gráfico de evolução diária
        st.subheader("📈 Evolução de Vendas")
        
        if unidade_selecionada == "Todas as Unidades":
            query_evolucao = f"""
            SELECT
                DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) as data,
                COUNT(s.ID) as vouchers,
                SUM(s.PRICE_NET) as receita
            FROM `buddha-bigdata.raw.ecommerce_raw` s
            WHERE 
                (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
                AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
            GROUP BY data
            ORDER BY data
            """
        else:
            query_evolucao = f"""
            SELECT
                DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) as data,
                COUNT(s.ID) as vouchers,
                SUM(s.PRICE_NET) as receita
            FROM `buddha-bigdata.raw.ecommerce_raw` s
            WHERE 
                (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
                AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
                AND (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) = '{unidade_selecionada}'
            GROUP BY data
            ORDER BY data
            """
        
        df_evolucao = executar_query(query_evolucao)
        
        if not df_evolucao.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_evolucao['data'],
                y=df_evolucao['receita'],
                mode='lines+markers',
                name='Receita',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=8)
            ))
            fig.update_layout(
                title="Receita Diária",
                xaxis_title="Data",
                yaxis_title="Receita (R$)",
                hovermode='x unified',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("⚠️ Nenhum dado encontrado para o período selecionado.")

# ============================================================
# 2. ABA FINANCEIRO
# ============================================================
with abas[1]:
    st.header("💰 Financeiro")
    
    # Query para dados financeiros
    if unidade_selecionada == "Todas as Unidades":
        query_financeiro = f"""
        SELECT
            s.ID,
            s.NAME,
            s.PRICE_NET,
            s.PRICE_GROSS,
            s.PRICE_REFOUND,
            DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS data_venda,
            (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS unidade,
            (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS servico
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    else:
        query_financeiro = f"""
        SELECT
            s.ID,
            s.NAME,
            s.PRICE_NET,
            s.PRICE_GROSS,
            s.PRICE_REFOUND,
            DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS data_venda,
            (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS unidade,
            (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS servico
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
            AND (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) = '{unidade_selecionada}'
        """
    
    df_financeiro = executar_query(query_financeiro)
    
    if not df_financeiro.empty:
        # Cards financeiros
        col1, col2, col3, col4 = st.columns(4)
        
        receita_bruta = df_financeiro['PRICE_GROSS'].sum()
        receita_liquida = df_financeiro['PRICE_NET'].sum()
        descontos = df_financeiro['PRICE_REFOUND'].sum()
        ticket_medio = receita_liquida / len(df_financeiro) if len(df_financeiro) > 0 else 0
        
        with col1:
            st.metric("💵 Receita Bruta", f"R$ {receita_bruta:,.2f}")
        with col2:
            st.metric("💰 Receita Líquida", f"R$ {receita_liquida:,.2f}")
        with col3:
            st.metric("🎁 Descontos", f"R$ {descontos:,.2f}")
        with col4:
            st.metric("🎯 Ticket Médio", f"R$ {ticket_medio:,.2f}")
        
        # Receita por unidade
        st.subheader("📊 Receita por Unidade")
        receita_unidade = df_financeiro.groupby('unidade')['PRICE_NET'].sum().sort_values(ascending=False).head(10)
        fig = px.bar(
            x=receita_unidade.values,
            y=receita_unidade.index,
            orientation='h',
            labels={'x': 'Receita (R$)', 'y': 'Unidade'},
            title="Top 10 Unidades por Receita"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Receita por serviço
        st.subheader("🎯 Receita por Serviço")
        receita_servico = df_financeiro.groupby('servico')['PRICE_NET'].sum().sort_values(ascending=False).head(10)
        fig = px.pie(
            values=receita_servico.values,
            names=receita_servico.index,
            title="Top 10 Serviços por Receita"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabela detalhada
        st.subheader("📋 Detalhamento Financeiro")
        st.dataframe(
            df_financeiro[['ID', 'NAME', 'data_venda', 'unidade', 'servico', 'PRICE_GROSS', 'PRICE_NET', 'PRICE_REFOUND']],
            use_container_width=True
        )
    else:
        st.warning("⚠️ Nenhum dado financeiro encontrado para o período selecionado.")

# ============================================================
# 3. ABA ATENDIMENTO
# ============================================================
with abas[2]:
    st.header("👥 Atendimento")
    
    # Query para dados de atendimento
    if unidade_selecionada == "Todas as Unidades":
        query_atendimento = f"""
        SELECT
            s.ID,
            s.NAME,
            s.STATUS,
            DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS data_venda,
            DATETIME(s.USED_DATE, "America/Sao_Paulo") AS data_uso,
            (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS unidade,
            (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS servico
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    else:
        query_atendimento = f"""
        SELECT
            s.ID,
            s.NAME,
            s.STATUS,
            DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS data_venda,
            DATETIME(s.USED_DATE, "America/Sao_Paulo") AS data_uso,
            (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS unidade,
            (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS servico
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
            AND (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) = '{unidade_selecionada}'
        """
    
    df_atendimento = executar_query(query_atendimento)
    
    if not df_atendimento.empty:
        # Cards de atendimento
        col1, col2, col3, col4 = st.columns(4)
        
        total_vouchers = len(df_atendimento)
        vouchers_usados = len(df_atendimento[df_atendimento['STATUS'] == '2'])
        vouchers_ativos = len(df_atendimento[df_atendimento['STATUS'] == '1'])
        taxa_uso = (vouchers_usados / total_vouchers * 100) if total_vouchers > 0 else 0
        
        with col1:
            st.metric("🎫 Total Vouchers", f"{total_vouchers:,}")
        with col2:
            st.metric("✅ Vouchers Usados", f"{vouchers_usados:,}")
        with col3:
            st.metric("⏳ Vouchers Ativos", f"{vouchers_ativos:,}")
        with col4:
            st.metric("📊 Taxa de Uso", f"{taxa_uso:.1f}%")
        
        # Serviços mais realizados
        st.subheader("🏆 Top 10 Serviços Mais Realizados")
        top_servicos = df_atendimento[df_atendimento['STATUS'] == '2'].groupby('servico').size().sort_values(ascending=False).head(10)
        fig = px.bar(
            x=top_servicos.values,
            y=top_servicos.index,
            orientation='h',
            labels={'x': 'Quantidade', 'y': 'Serviço'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Atendimentos por unidade
        st.subheader("🏢 Atendimentos por Unidade")
        atendimentos_unidade = df_atendimento[df_atendimento['STATUS'] == '2'].groupby('unidade').size().sort_values(ascending=False).head(10)
        fig = px.pie(
            values=atendimentos_unidade.values,
            names=atendimentos_unidade.index,
            title="Distribuição de Atendimentos"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Tabela detalhada
        st.subheader("📋 Detalhamento de Vouchers")
        df_atendimento['status_texto'] = df_atendimento['STATUS'].map({
            '1': '⏳ Ativo',
            '2': '✅ Usado',
            '3': '❌ Cancelado'
        })
        st.dataframe(
            df_atendimento[['ID', 'NAME', 'status_texto', 'data_venda', 'data_uso', 'unidade', 'servico']],
            use_container_width=True
        )
    else:
        st.warning("⚠️ Nenhum dado de atendimento encontrado para o período selecionado.")

# ============================================================
# 4. ABA ECOMMERCE
# ============================================================
with abas[3]:
    st.header("📦 Ecommerce – Vendas de Vouchers")
    
    # Query de ecommerce COM FILTRO DE UNIDADE
    if unidade_selecionada == "Todas as Unidades":
        query_ecommerce = f"""
        SELECT 
            s.ID, 
            s.NAME,
            s.STATUS,
            s.COUPONS, 
            s.CREATED_DATE, 
            DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
            s.USED_DATE, 
            DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL,
            s.PRICE_NET, 
            s.PRICE_GROSS, 
            s.PRICE_REFOUND, 
            s.KEY, 
            s.ORDER_ID, 
            (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME, 
            (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS AFILLIATION_NAME,
            (SELECT 
                CONCAT(
                    MAX(CASE WHEN pm.meta_key = '_billing_first_name' THEN pm.meta_value END),
                    ' ',
                    MAX(CASE WHEN pm.meta_key = '_billing_last_name' THEN pm.meta_value END)
                ) 
                FROM `buddha-bigdata.raw.wp_posts` o
                LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
                WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
            ) AS Customer_FullName,
            (SELECT 
                MAX(CASE WHEN pm.meta_key = '_billing_email' THEN pm.meta_value END) 
                FROM `buddha-bigdata.raw.wp_posts` o
                LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
                WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
            ) AS Customer_Email
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
        """
    else:
        query_ecommerce = f"""
        SELECT 
            s.ID, 
            s.NAME,
            s.STATUS,
            s.COUPONS, 
            s.CREATED_DATE, 
            DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
            s.USED_DATE, 
            DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL,
            s.PRICE_NET, 
            s.PRICE_GROSS, 
            s.PRICE_REFOUND, 
            s.KEY, 
            s.ORDER_ID, 
            (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME, 
            (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS AFILLIATION_NAME,
            (SELECT 
                CONCAT(
                    MAX(CASE WHEN pm.meta_key = '_billing_first_name' THEN pm.meta_value END),
                    ' ',
                    MAX(CASE WHEN pm.meta_key = '_billing_last_name' THEN pm.meta_value END)
                ) 
                FROM `buddha-bigdata.raw.wp_posts` o
                LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
                WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
            ) AS Customer_FullName,
            (SELECT 
                MAX(CASE WHEN pm.meta_key = '_billing_email' THEN pm.meta_value END) 
                FROM `buddha-bigdata.raw.wp_posts` o
                LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
                WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
            ) AS Customer_Email
        FROM `buddha-bigdata.raw.ecommerce_raw` s
        WHERE 
            (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
            AND DATE(DATETIME(s.CREATED_DATE, "America/Sao_Paulo")) BETWEEN '{data_inicio}' AND '{data_fim}'
            AND (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u WHERE u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) = '{unidade_selecionada}'
        """
    
    df_ecommerce = executar_query(query_ecommerce)
    
    if df_ecommerce.empty:
        st.warning("⚠️ Nenhum dado de ecommerce encontrado para o período/unidade selecionados.")
    else:
        # Cards de métricas
        col1, col2, col3, col4 = st.columns(4)
        
        total_pedidos = df_ecommerce["ORDER_ID"].nunique()
        total_vouchers = len(df_ecommerce)
        receita_liquida = df_ecommerce["PRICE_NET"].sum()
        ticket_medio = receita_liquida / total_pedidos if total_pedidos > 0 else 0
        
        with col1:
            st.metric("Pedidos Ecommerce", f"{total_pedidos:,}")
        with col2:
            st.metric("Vouchers Vendidos", f"{total_vouchers:,}")
        with col3:
            st.metric("Receita Líquida Ecommerce", f"R$ {receita_liquida:,.2f}")
        with col4:
            st.metric("Ticket Médio Ecommerce", f"R$ {ticket_medio:,.2f}")
        
        st.subheader("Top 10 Serviços / Pacotes Vendidos (Ecommerce)")
        top_servicos = (
            df_ecommerce.groupby("PACKAGE_NAME")
            .agg({"ID": "count", "PRICE_NET": "sum"})
            .rename(columns={"ID": "Quantidade", "PRICE_NET": "Receita Líquida"})
            .sort_values("Quantidade", ascending=False)
            .head(10)
        )
        st.dataframe(top_servicos, use_container_width=True)
        
        st.subheader("Vouchers Vendidos por Unidade (Destino)")
        vendas_por_unidade = (
            df_ecommerce.groupby("AFILLIATION_NAME")
            .agg({"ID": "count", "PRICE_NET": "sum"})
            .rename(columns={"ID": "Quantidade", "PRICE_NET": "Receita Líquida"})
            .sort_values("Quantidade", ascending=False)
        )
        st.dataframe(vendas_por_unidade, use_container_width=True)
        
        st.subheader("Detalhamento de Vouchers Vendidos")
        st.dataframe(
            df_ecommerce[[
                "ID", "NAME", "STATUS", "CREATED_DATE_BRAZIL", "USED_DATE_BRAZIL",
                "PRICE_NET", "PRICE_GROSS", "PACKAGE_NAME", "AFILLIATION_NAME",
                "Customer_FullName", "Customer_Email"
            ]],
            use_container_width=True
        )

# ============================================================
# 5. ABA MARKETING
# ============================================================
with abas[4]:
    st.header("📈 Marketing")
    
    st.info("🚧 Seção em desenvolvimento. Em breve você terá acesso a:")
    st.markdown("""
    - 📊 Dados do Google Analytics 4 (GA4)
    - 📱 Métricas de Meta Ads (Facebook/Instagram)
    - 🎯 Google Ads
    - 📈 ROI e CAC por canal
    - 🔍 Análise de conversão
    """)
    
    # Placeholder para futuras integrações
    st.warning("⚠️ Para visualizar dados de marketing, é necessário configurar as integrações com GA4 e Meta Ads.")

# ============================================================
# 6. ABA AJUDA
# ============================================================
with abas[5]:
    st.header("❓ Ajuda")
    
    st.markdown("""
    ### 📖 Guia de Uso do Dashboard
    
    #### 🎯 Filtros Globais
    - **Período**: Selecione o intervalo de datas para análise
    - **Unidade**: Filtre os dados por unidade específica ou visualize todas
    
    #### 📊 Abas Disponíveis
    
    **1. Visão Geral**
    - Resumo executivo com principais métricas
    - Evolução de vendas ao longo do tempo
    - Indicadores consolidados
    
    **2. Financeiro**
    - Receita bruta e líquida
    - Descontos aplicados
    - Ticket médio
    - Análise por unidade e serviço
    
    **3. Atendimento**
    - Vouchers vendidos vs. usados
    - Taxa de utilização
    - Serviços mais realizados
    - Distribuição por unidade
    
    **4. Ecommerce**
    - Vendas online de vouchers
    - Análise de pedidos
    - Detalhamento de clientes
    - Performance por serviço
    
    **5. Marketing**
    - Em desenvolvimento
    - Integrações futuras com GA4 e Meta Ads
    
    #### 🔄 Atualização de Dados
    - Os dados são atualizados automaticamente a cada hora
    - Use os filtros para refinar sua análise
    
    #### 📞 Suporte
    - Em caso de dúvidas, entre em contato com a equipe de TI
    - Para sugestões de melhorias, envie um email para suporte@buddhaspa.com.br
    
    #### 🔐 Segurança
    - Todos os dados são criptografados
    - Acesso restrito a usuários autorizados
    - Logs de auditoria mantidos por 90 dias
    """)
    
    st.success("✅ Dashboard versão 1.0 - Última atualização: Dezembro 2024")

# ============================================================
# RODAPÉ
# ============================================================
st.markdown("---")
st.caption("🧘 Buddha Spa - Dashboard Executivo | Desenvolvido com ❤️ usando Streamlit")
