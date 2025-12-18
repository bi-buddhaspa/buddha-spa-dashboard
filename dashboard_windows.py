import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Buddha Spa Analytics",
    page_icon="🧘",
    layout="wide"
)

# -----------------------------------------------------------------------------
# ESTILO
# -----------------------------------------------------------------------------
st.markdown("""
    <style>
    .stApp {
        background-color: #F5F0E6;
    }
    [data-testid="stSidebar"] {
        background-color: #FFFFFF;
    }
    .stMetric {
        background-color: #FFFFFF;
        padding: 18px;
        border-radius: 10px;
        border: 2px solid #8B0000;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        min-height: 110px;
    }
    .stMetric label {
        color: #8B0000 !important;
        font-size: 0.85rem !important;
        font-weight: 600 !important;
        white-space: normal !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #333333 !important;
        font-size: 1.6rem !important;
        font-weight: 700 !important;
        line-height: 1.1 !important;
        white-space: normal !important;
    }
    h1 {
        color: #8B0000 !important;
        font-weight: 700 !important;
    }
    h2, h3 {
        color: #8B0000 !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #FFFFFF;
        padding: 10px;
        border-radius: 10px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: #F5F0E6;
        color: #8B0000;
        border-radius: 5px;
        padding: 10px 20px;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #8B0000;
        color: #FFFFFF;
    }
    .stButton > button,
    .stDownloadButton > button {
        background-color: #8B0000;
        color: #FFFFFF;
        border-radius: 5px;
        border: none;
        font-weight: 600;
    }
    .stButton > button:hover,
    .stDownloadButton > button:hover {
        background-color: #A52A2A;
    }
    .stDataFrame {
        background-color: #FFFFFF;
        border-radius: 10px;
        padding: 10px;
    }
    hr {
        border-color: #8B0000;
    }
    .stMultiSelect [data-baseweb="tag"] {
        background-color: #8B0000;
    }
    .stSlider [data-baseweb="slider"] [role="slider"] {
        background-color: #8B0000;
    }
    .stCheckbox [data-baseweb="checkbox"] {
        border-color: #8B0000;
    }
    .stCheckbox [data-baseweb="checkbox"]:checked {
        background-color: #8B0000;
    }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# CONEXÃO BIGQUERY
# -----------------------------------------------------------------------------
@st.cache_resource
def get_bigquery_client():
    from google.oauth2 import service_account
    from google.cloud import bigquery

    if 'gcp_service_account' in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=credentials, project='buddha-bigdata')
    else:
        return bigquery.Client(project='buddha-bigdata')

# -----------------------------------------------------------------------------
# FUNÇÕES DE DADOS (USANDO itens_atendimentos_analytics)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_atendimentos(data_inicio, data_fim):
    """
    Carrega os itens de atendimentos detalhados.
    Ajuste os nomes das colunas abaixo se forem diferentes na tabela real.
    """
    client = get_bigquery_client()
    query = f"""
    SELECT
        unidade,
        DATE(data_atendimento) AS data_atendimento,
        forma_pagamento,
        servico,
        profissional,   -- Terapeuta
        cliente,
        valor_bruto,
        valor_liquido
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_unidades():
    client = get_bigquery_client()
    query = """
    SELECT DISTINCT LOWER(unidade) AS unidade
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    ORDER BY unidade
    LIMIT 100
    """
    return client.query(query).to_dataframe()['unidade'].tolist()

@st.cache_data(ttl=3600)
def load_ecommerce_data(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
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
        (SELECT u.post_title FROM `buddha-bigdata.raw.wp_posts` u 
         WHERE u.post_type = 'unidade' 
           AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS AFILLIATION_NAME,
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
        ) AS Customer_Email,
        (SELECT 
            MAX(CASE WHEN pm.meta_key = '_billing_phone' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Phone,
        (SELECT 
            MAX(CASE WHEN pm.meta_key = '_billing_cellphone' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Cellphone,
        (SELECT 
            MAX(CASE WHEN usermeta.meta_key = 'billing_cpf' THEN usermeta.meta_value END) 
            FROM `buddha-bigdata.raw.wp_postmeta` pm
            LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta 
                ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
            WHERE pm.meta_key = '_customer_user' 
              AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_CPF,
        (SELECT 
            MAX(CASE WHEN usermeta.meta_key = 'billing_cnpj' THEN usermeta.meta_value END) 
            FROM `buddha-bigdata.raw.wp_postmeta` pm
            LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta 
                ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
            WHERE pm.meta_key = '_customer_user' 
              AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_CNPJ,
        (SELECT 
            MAX(CASE WHEN pm.meta_key = '_billing_address_1' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Address_1,
        (SELECT 
            MAX(CASE WHEN pm.meta_key = '_billing_number' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Number,
        (SELECT 
            MAX(CASE WHEN pm.meta_key = '_billing_postcode' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Postcode,
        (SELECT 
            MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_City,
        (SELECT 
            MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_State,
        (SELECT 
            MAX(CASE WHEN usermeta.meta_key = 'birthdate' THEN usermeta.meta_value END) 
            FROM `buddha-bigdata.raw.wp_postmeta` pm
            LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta 
                ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
            WHERE pm.meta_key = '_customer_user' 
              AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Birthdate
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    WHERE 
        s.CREATED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.CREATED_DATE <= TIMESTAMP('{data_fim} 23:59:59', 'America/Sao_Paulo')
        AND s.STATUS IN ('1','2','3')
    """
    return client.query(query).to_dataframe()

# -----------------------------------------------------------------------------
# SIDEBAR – FILTROS
# -----------------------------------------------------------------------------
st.sidebar.title("Filtros")

col1, col2 = st.sidebar.columns(2)
data_inicio = col1.date_input("De:", value=datetime(2025, 9, 1))
data_fim = col2.date_input("Até:", value=datetime(2025, 9, 30))

try:
    unidades_disponiveis = load_unidades()
    unidades_selecionadas = st.sidebar.multiselect(
        "Unidades:",
        options=unidades_disponiveis,
        default=None
    )
except Exception as e:
    st.error(f"Erro ao carregar unidades: {e}")
    st.stop()

# -----------------------------------------------------------------------------
# CARREGAR DADOS DE ATENDIMENTOS
# -----------------------------------------------------------------------------
with st.spinner("Carregando dados de atendimentos..."):
    try:
        df = load_atendimentos(data_inicio, data_fim)
        if unidades_selecionadas:
            df = df[df['unidade'].str.lower().isin(unidades_selecionadas)]
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

if df.empty:
    st.warning("Sem dados de atendimentos para o período/unidades selecionados.")
    st.stop()

# Normalizar nome das colunas principais
data_col = 'data_atendimento'
valor_col = 'valor_liquido' if 'valor_liquido' in df.columns else (
    'valor_bruto' if 'valor_bruto' in df.columns else None
)

if valor_col is None:
    st.error("Não encontrei colunas de valor (valor_liquido ou valor_bruto) na tabela de atendimentos.")
    st.stop()

# -----------------------------------------------------------------------------
# HEADER
# -----------------------------------------------------------------------------
st.title("Buddha Spa - Dashboard de Unidades")
st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

# -----------------------------------------------------------------------------
# KPIs PRINCIPAIS (VISÃO GERAL)
# -----------------------------------------------------------------------------
receita_total = df[valor_col].sum()
qtd_atendimentos = int(len(df))
qtd_clientes = int(df['cliente'].nunique()) if 'cliente' in df.columns else 0
ticket_medio = receita_total / qtd_atendimentos if qtd_atendimentos > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Receita Total", f"R$ {receita_total:,.2f}")
col2.metric("Quantidade de Atendimentos", f"{qtd_atendimentos:d}")
col3.metric("Clientes Únicos", f"{qtd_clientes:d}")
col4.metric("Ticket Médio por Atendimento", f"R$ {ticket_medio:,.2f}")

st.divider()

# -----------------------------------------------------------------------------
# TABS PRINCIPAIS
# -----------------------------------------------------------------------------
tab_visao, tab_atend, tab_fin, tab_mkt, tab_selfservice, tab_gloss = st.tabs(
    ["Visão Geral", "Atendimento", "Financeiro", "Marketing & Ecommerce", "Self-Service", "Ajuda / Glossário"]
)

# ---------------------- TAB: VISÃO GERAL -------------------------
with tab_visao:
    st.subheader("Evolução da Receita")

    df_evolucao = (
        df.groupby(data_col)[valor_col]
        .sum()
        .reset_index()
        .sort_values(data_col)
    )

    fig = px.line(df_evolucao, x=data_col, y=valor_col, markers=True)
    fig.update_traces(line_color='#8B0000', marker=dict(color='#8B0000'))
    fig.update_layout(
        xaxis_title="Data",
        yaxis_title="Receita (R$)",
        height=400,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6'
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Receita por Unidade")

    df_unidades = (
        df.groupby('unidade')[valor_col]
        .sum()
        .reset_index()
        .sort_values(valor_col, ascending=False)
    )

    fig_u = px.bar(
        df_unidades,
        x=valor_col,
        y='unidade',
        orientation='h',
        labels={valor_col: 'Receita (R$)', 'unidade': 'Unidade'}
    )
    fig_u.update_traces(marker_color='#8B0000')
    fig_u.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        height=450
    )
    st.plotly_chart(fig_u, use_container_width=True)

# ---------------------- TAB: ATENDIMENTO -------------------------
with tab_atend:
    st.subheader("Performance por Terapeuta")
    st.caption("Aqui concentramos tudo o que é de Terapeuta (sem acento circunflexo no rótulo).")

    if 'profissional' in df.columns:
        df_terap = (
            df.groupby(['unidade', 'profissional'])
            .agg(
                receita=(valor_col, 'sum'),
                qtd_atendimentos=('cliente', 'count' if 'cliente' in df.columns else 'size'),
                clientes_unicos=('cliente', 'nunique') if 'cliente' in df.columns else ('unidade', 'size')
            )
            .reset_index()
        )
        df_terap['ticket_medio'] = df_terap['receita'] / df_terap['qtd_atendimentos']

        df_terap = df_terap.sort_values('receita', ascending=False)

        # TOTAL no final
        totais = {
            'unidade': 'TOTAL',
            'profissional': 'TOTAL',
            'receita': df_terap['receita'].sum(),
            'qtd_atendimentos': df_terap['qtd_atendimentos'].sum(),
            'clientes_unicos': df_terap['clientes_unicos'].sum() if 'clientes_unicos' in df_terap.columns else None,
        }
        totais['ticket_medio'] = (
            totais['receita'] / totais['qtd_atendimentos'] if totais['qtd_atendimentos'] > 0 else 0
        )
        df_terap_total = pd.concat([df_terap, pd.DataFrame([totais])], ignore_index=True)

        col_a, col_b = st.columns([2, 1])

        with col_a:
            st.markdown("### Top Terapeutas por Receita")
            top_terap = df_terap.head(15)
            fig_t = px.bar(
                top_terap,
                x='receita',
                y='profissional',
                color='unidade',
                orientation='h',
                labels={
                    'receita': 'Receita (R$)',
                    'profissional': 'Terapeuta',
                    'unidade': 'Unidade'
                }
            )
            fig_t.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=500
            )
            st.plotly_chart(fig_t, use_container_width=True)

        with col_b:
            st.markdown("### Tabela de Performance por Terapeuta (com TOTAL)")
            st.dataframe(
                df_terap_total.style.format({
                    'receita': 'R$ {:,.2f}',
                    'qtd_atendimentos': '{:,.0f}',
                    'clientes_unicos': '{:,.0f}',
                    'ticket_medio': 'R$ {:,.2f}'
                }),
                use_container_width=True,
                height=500
            )
    else:
        st.info("A coluna 'profissional' não foi encontrada em itens_atendimentos_analytics. Confirme o nome dessa coluna na tabela.")

    st.markdown("---")
    st.subheader("Principais Serviços (com % do Total)")

    if 'servico' in df.columns:
        df_servicos = (
            df.groupby('servico')[valor_col]
            .sum()
            .reset_index()
            .rename(columns={valor_col: 'receita'})
        )
        df_servicos['perc_receita'] = df_servicos['receita'] / df_servicos['receita'].sum()
        df_servicos = df_servicos.sort_values('receita', ascending=False).head(15)

        col1_s, col2_s = st.columns([2, 1])

        with col1_s:
            fig_s = px.bar(
                df_servicos,
                x='receita',
                y='servico',
                orientation='h',
                text=df_servicos['perc_receita'].map(lambda x: f"{x*100:.1f}%"),
                labels={'receita': 'Receita (R$)', 'servico': 'Serviço'}
            )
            fig_s.update_traces(marker_color='#8B0000', textposition='outside')
            fig_s.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=500
            )
            st.plotly_chart(fig_s, use_container_width=True)

        with col2_s:
            st.dataframe(
                df_servicos.rename(columns={
                    'servico': 'Serviço',
                    'receita': 'Receita',
                    'perc_receita': '% Receita'
                }).style.format({
                    'Receita': 'R$ {:,.2f}',
                    '% Receita': '{:.1%}'
                }),
                use_container_width=True,
                height=500
            )
    else:
        st.info("A coluna 'servico' não foi encontrada em itens_atendimentos_analytics.")

# ---------------------- TAB: FINANCEIRO -------------------------
with tab_fin:
    st.subheader("Resumo Financeiro da Unidade")

    colf1, colf2, colf3 = st.columns(3)
    colf1.metric("Receita Total (Atendimentos)", f"R$ {receita_total:,.2f}")
    colf2.metric("Quantidade de Atendimentos", f"{qtd_atendimentos:d}")
    colf3.metric("Ticket Médio Unidade", f"R$ {ticket_medio:,.2f}")

    st.markdown("---")
    st.subheader("Receita por Unidade (Atendimento Presencial)")

    df_fin_unid = (
        df.groupby('unidade')[valor_col]
        .sum()
        .reset_index()
        .rename(columns={valor_col: 'receita'})
        .sort_values('receita', ascending=False)
    )

    fig_fu = px.bar(
        df_fin_unid,
        x='receita',
        y='unidade',
        orientation='h',
        labels={'receita': 'Receita (R$)', 'unidade': 'Unidade'}
    )
    fig_fu.update_traces(marker_color='#A52A2A')
    fig_fu.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        height=450
    )
    st.plotly_chart(fig_fu, use_container_width=True)

    st.markdown("---")
    st.caption(
        "Obs.: Comparação com cluster, Receita Ajustada por Sala e Taxa de Ocupação "
        "dependem de tabelas adicionais (estrutura física, nº de salas e horários Belle)."
    )

# ---------------------- TAB: MARKETING & ECOMMERCE -------------------------
with tab_mkt:
    st.subheader("Ecommerce – Vendas de Vouchers")
    st.caption(
        "Aqui trazemos os indicadores de ecommerce (rede + franquias) com base em `raw.ecommerce_raw`.\n"
        "Sessões página, cliques WhatsApp, Ads e Meta Ads entram depois com GA4/Ads plugados."
    )

    with st.spinner("Carregando dados de ecommerce..."):
        df_ecom = load_ecommerce_data(data_inicio, data_fim)

    if df_ecom.empty:
        st.warning("Sem dados de ecommerce para o período selecionado.")
    else:
        df_ecom['COUPONS'] = pd.to_numeric(df_ecom['COUPONS'], errors='coerce')
        df_ecom['PRICE_GROSS'] = pd.to_numeric(df_ecom['PRICE_GROSS'], errors='coerce')
        df_ecom['PRICE_NET'] = pd.to_numeric(df_ecom['PRICE_NET'], errors='coerce')

        if 'PACKAGE_NAME' in df_ecom.columns:
            df_ecom['PACKAGE_NAME'] = df_ecom['PACKAGE_NAME'].fillna(df_ecom['NAME'])
        else:
            df_ecom['PACKAGE_NAME'] = df_ecom['NAME']

        if 'AFILLIATION_NAME' not in df_ecom.columns:
            df_ecom['AFILLIATION_NAME'] = "Sem Unidade"

        colm1, colm2, colm3, colm4 = st.columns(4)

        total_pedidos = int(df_ecom['ID'].nunique())
        total_vouchers = int(len(df_ecom))
        receita_bruta_e = df_ecom['PRICE_GROSS'].fillna(0).sum()
        receita_liquida_e = df_ecom['PRICE_NET'].fillna(0).sum()
        ticket_medio_e = receita_liquida_e / total_pedidos if total_pedidos > 0 else 0

        colm1.metric("Pedidos Ecommerce", f"{total_pedidos:,.0f}")
        colm2.metric("Vouchers Vendidos", f"{total_vouchers:,.0f}")
        colm3.metric("Receita Líquida Ecommerce", f"R$ {receita_liquida_e:,.2f}")
        colm4.metric("Ticket Médio Ecommerce", f"R$ {ticket_medio_e:,.2f}")

        st.markdown("---")
        st.markdown("### Top 10 Serviços / Pacotes Vendidos (Ecommerce)")

        df_serv = (
            df_ecom
            .groupby('PACKAGE_NAME')
            .agg(
                qtde_vouchers=('ID', 'count'),
                receita_liquida=('PRICE_NET', 'sum')
            )
            .reset_index()
            .sort_values('qtde_vouchers', ascending=False)
            .head(10)
        )

        col_a, col_b = st.columns([2, 1])

        with col_a:
            fig_serv = px.bar(
                df_serv,
                x='qtde_vouchers',
                y='PACKAGE_NAME',
                orientation='h',
                labels={'qtde_vouchers': 'Qtd Vouchers', 'PACKAGE_NAME': 'Serviço / Pacote'},
                text='qtde_vouchers'
            )
            fig_serv.update_traces(marker_color='#8B0000', textposition='outside')
            fig_serv.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=450
            )
            st.plotly_chart(fig_serv, use_container_width=True)

        with col_b:
            st.dataframe(
                df_serv.rename(columns={
                    'PACKAGE_NAME': 'Serviço / Pacote',
                    'qtde_vouchers': 'Qtd Vouchers',
                    'receita_liquida': 'Receita Líquida'
                }).style.format({
                    'Qtd Vouchers': '{:,.0f}',
                    'Receita Líquida': 'R$ {:,.2f}'
                }),
                use_container_width=True,
                height=450
            )

        st.markdown("---")
        st.markdown("### Receita Líquida por Unidade (Ecommerce)")

        df_af = (
            df_ecom
            .groupby('AFILLIATION_NAME')['PRICE_NET']
            .sum()
            .reset_index()
            .sort_values('PRICE_NET', ascending=False)
            .head(15)
        )

        fig_af = px.bar(
            df_af,
            x='PRICE_NET',
            y='AFILLIATION_NAME',
            orientation='h',
            labels={'PRICE_NET': 'Receita Líquida (R$)', 'AFILLIATION_NAME': 'Unidade'},
        )
        fig_af.update_traces(marker_color='#A52A2A')
        fig_af.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=450
        )
        st.plotly_chart(fig_af, use_container_width=True)

# ---------------------- TAB: SELF-SERVICE -------------------------
with tab_selfservice:
    st.subheader("Monte Sua Própria Análise (Self-Service)")
    st.caption("Escolha as dimensões e métricas – estilo QlikView, usando a base de atendimentos.")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### Agrupar Por (Linhas)")
        dimensoes = st.multiselect(
            "Selecione uma ou mais dimensões:",
            ["Data", "Unidade", "Forma de Pagamento", "Serviço", "Terapeuta", "Cliente"],
            default=["Unidade"],
            help="As linhas da sua tabela"
        )

    with c2:
        st.markdown("### Métricas (Colunas)")
        metricas = st.multiselect(
            "Selecione as métricas que deseja ver:",
            [
                "Receita Total",
                "Quantidade de Atendimentos",
                "Ticket Médio",
                "Clientes Únicos",
                "Maior Valor",
                "Menor Valor"
            ],
            default=["Receita Total", "Quantidade de Atendimentos", "Ticket Médio"],
            help="As colunas com os valores"
        )

    if not dimensoes:
        st.warning("Selecione pelo menos uma dimensão para agrupar.")
    elif not metricas:
        st.warning("Selecione pelo menos uma métrica para exibir.")
    else:
        dim_map = {
            "Data": data_col,
            "Unidade": "unidade",
            "Forma de Pagamento": "forma_pagamento",
            "Serviço": "servico",
            "Terapeuta": "profissional",
            "Cliente": "cliente"
        }

        colunas_agrupamento = [dim_map[d] for d in dimensoes if dim_map[d] in df.columns]

        if not colunas_agrupamento:
            st.error("Nenhuma das dimensões selecionadas existe na base de atendimentos. Ajuste os nomes no SELECT se necessário.")
        else:
            agg_dict = {}
            if "Receita Total" in metricas:
                agg_dict['receita_total'] = (valor_col, 'sum')
            if "Quantidade de Atendimentos" in metricas:
                agg_dict['qtd_atendimentos'] = (valor_col, 'count')
            if "Clientes Únicos" in metricas and 'cliente' in df.columns:
                agg_dict['clientes_unicos'] = ('cliente', 'nunique')
            if "Maior Valor" in metricas:
                agg_dict['maior_valor'] = (valor_col, 'max')
            if "Menor Valor" in metricas:
                agg_dict['menor_valor'] = (valor_col, 'min')

            df_custom = df.groupby(colunas_agrupamento).agg(**agg_dict).reset_index()

            if "Ticket Médio" in metricas:
                if 'receita_total' in df_custom.columns and 'qtd_atendimentos' in df_custom.columns:
                    df_custom['ticket_medio'] = df_custom['receita_total'] / df_custom['qtd_atendimentos']

            rename_map = {
                'receita_total': 'Receita Total',
                'qtd_atendimentos': 'Qtd Atendimentos',
                'clientes_unicos': 'Clientes Únicos',
                'ticket_medio': 'Ticket Médio',
                'maior_valor': 'Maior Valor',
                'menor_valor': 'Menor Valor',
                data_col: 'Data',
                'unidade': 'Unidade',
                'forma_pagamento': 'Forma Pagamento',
                'servico': 'Serviço',
                'profissional': 'Terapeuta',
                'cliente': 'Cliente'
            }
            df_custom = df_custom.rename(columns=rename_map)

            colunas_numericas = [
                c for c in df_custom.columns
                if c not in ['Data', 'Unidade', 'Forma Pagamento', 'Serviço', 'Terapeuta', 'Cliente']
                and pd.api.types.is_numeric_dtype(df_custom[c])
            ]
            if colunas_numericas:
                df_custom = df_custom.sort_values(colunas_numericas[0], ascending=False)

            c1b, c2b = st.columns([2, 1])
            with c1b:
                limite = st.slider("Limitar resultados:", 5, 100, 20, 5)
            with c2b:
                mostrar_totais = st.checkbox("Mostrar totais", value=True)

            df_display = df_custom.head(limite).copy()

            if mostrar_totais and len(df_display) > 0:
                totais = {}
                for col in df_display.columns:
                    if col in ['Data', 'Unidade', 'Forma Pagamento', 'Serviço', 'Terapeuta', 'Cliente']:
                        totais[col] = 'TOTAL'
                    elif pd.api.types.is_numeric_dtype(df_display[col]):
                        if col == 'Ticket Médio':
                            if 'Receita Total' in df_display.columns and 'Qtd Atendimentos' in df_display.columns:
                                totais[col] = (
                                    df_display['Receita Total'].sum() /
                                    df_display['Qtd Atendimentos'].sum()
                                )
                            else:
                                totais[col] = df_display[col].mean()
                        elif col in ['Clientes Únicos', 'Qtd Atendimentos']:
                            totais[col] = df_display[col].sum()
                        elif col in ['Maior Valor', 'Menor Valor']:
                            totais[col] = (
                                df_display[col].max() if 'Maior' in col else df_display[col].min()
                            )
                        else:
                            totais[col] = df_display[col].sum()

                df_display = pd.concat([df_display, pd.DataFrame([totais])], ignore_index=True)

            format_dict = {}
            for col in df_display.columns:
                if pd.api.types.is_numeric_dtype(df_display[col]):
                    if col in ['Receita Total', 'Ticket Médio', 'Maior Valor', 'Menor Valor']:
                        format_dict[col] = 'R$ {:,.2f}'
                    else:
                        format_dict[col] = '{:,.0f}'

            st.markdown("---")
            st.markdown(f"### Resultado ({len(df_custom)} linhas no total)")

            st.dataframe(
                df_display.style.format(format_dict),
                use_container_width=True,
                height=400
            )

            csv = df_custom.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV Completo (Self-Service)",
                csv,
                f"buddha_selfservice_{data_inicio}_{data_fim}.csv",
                "text/csv"
            )

# ---------------------- TAB: AJUDA / GLOSSÁRIO -------------------------
with tab_gloss:
    st.subheader("Ajuda / Glossário de Métricas")

    st.markdown("""
    **Receita Total (Atendimentos)**  
    Soma de todos os valores (líquidos ou brutos, conforme campo usado) de atendimentos no período selecionado.

    **Quantidade de Atendimentos**  
    Número de registros de atendimentos no período (cada linha = 1 atendimento).

    **Clientes Únicos**  
    Número de clientes distintos atendidos no período.

    **Ticket Médio por Atendimento**  
    $$\\text{Ticket Médio} = \\frac{\\text{Receita Total}}{\\#\\text{Atendimentos}}$$

    **Receita Ecommerce (Rede / Franquias)**  
    Soma dos valores de vouchers vendidos (PRICE_NET) no ecommerce.

    **Pedidos Ecommerce**  
    Número de pedidos únicos (ID do pedido) no ecommerce.

    **Vouchers Vendidos**  
    Quantidade total de vouchers (cada linha da tabela de ecommerce é um voucher).

    **Pontos do pedido do chefe que dependerão de dados adicionais:**

    - **Taxa de Ocupação (unidade e por Terapeuta)**  
      Precisa de horário de funcionamento do SPA + escala Belle + nº de salas de atendimento.
      Fórmula pedida:  
      $$\\text{Taxa de Ocupação} = \\frac{\\text{Horas de Atendimento}}{\\text{Horas Disponíveis}}$$

    - **Comparação com Cluster (média, máximo, mínimo, ranking)**  
      Precisa de uma tabela que diga em que *cluster* cada unidade está.

    - **Investimentos em Marketing (locais)**  
      Precisa de uma tabela/planilha onde o franqueado informe os gastos de marketing.

    - **Suporte (Visitas, Eventos e Desenvolvimento)**  
      Precisa de uma tabela de visitas/eventos com datas, unidades e participação.
    """)

# -----------------------------------------------------------------------------
# TABELA DETALHADA DE ATENDIMENTOS
# -----------------------------------------------------------------------------
st.divider()
st.subheader("Dados Detalhados de Atendimentos (Top 100 linhas)")

cols_detalhe = [c for c in [data_col, 'unidade', 'cliente', 'servico', valor_col] if c in df.columns]
df_view = df[cols_detalhe].head(100)

st.dataframe(df_view, use_container_width=True, height=300)

csv_atend = df.to_csv(index=False).encode('utf-8')
st.download_button(
    "Download CSV Completo de Atendimentos",
    csv_atend,
    f"buddha_atendimentos_{data_inicio}_{data_fim}.csv",
    "text/csv"
)

st.caption("Buddha Spa Dashboard – Versão alinhada ao pedido do chefe (dentro do que os dados atuais permitem).")
