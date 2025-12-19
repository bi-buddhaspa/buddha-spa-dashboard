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
# SISTEMA DE AUTENTICAÇÃO SIMPLES
# -----------------------------------------------------------------------------
USUARIOS = {
    'joao.silva@buddhaspa.com.br': {
        'senha': '12345',
        'nome': 'João Silva',
        'unidade': 'buddha spa - higienópolis'
    },
    'leandro.santos@buddhaspa.com.br': {
        'senha': '625200',
        'nome': 'Leandro Santos',
        'unidade': 'TODAS'
    }
}

if 'autenticado' not in st.session_state:
    st.session_state.autenticado = False
    st.session_state.usuario = None
    st.session_state.nome = None
    st.session_state.unidade = None

def fazer_login(email, senha):
    if email in USUARIOS and USUARIOS[email]['senha'] == senha:
        st.session_state.autenticado = True
        st.session_state.usuario = email
        st.session_state.nome = USUARIOS[email]['nome']
        st.session_state.unidade = USUARIOS[email]['unidade']
        return True
    return False

def fazer_logout():
    st.session_state.autenticado = False
    st.session_state.usuario = None
    st.session_state.nome = None
    st.session_state.unidade = None

if not st.session_state.autenticado:
    st.markdown("""
        <div style='text-align: center; padding: 50px;'>
            <h1 style='color: #8B0000;'>🧘 Portal de Franqueados - Buddha Spa</h1>
            <p style='color: #666;'>Faça login para acessar o dashboard</p>
        </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])

    with col2:
        with st.form("login_form"):
            email = st.text_input("Email", placeholder="seu.email@buddhaspa.com.br")
            senha = st.text_input("Senha", type="password", placeholder="Digite sua senha")
            submit = st.form_submit_button("Entrar", use_container_width=True)

            if submit:
                if fazer_login(email, senha):
                    st.success(f"Bem-vindo, {st.session_state.nome}!")
                    st.rerun()
                else:
                    st.error("Email ou senha incorretos")

    st.stop()

unidade_usuario = st.session_state.unidade
is_admin = (unidade_usuario == 'TODAS')

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
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #333333 !important;
        font-size: 1.6rem !important;
        font-weight: 700 !important;
        line-height: 1.1 !important;
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
# FUNÇÕES DE DADOS – ATENDIMENTO / FINANCEIRO
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_atendimentos(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()

    filtro_unidade = ""
    if unidade_filtro and unidade_filtro != 'TODAS':
        filtro_unidade = f"AND LOWER(unidade) = LOWER('{unidade_filtro}')"

    query = f"""
    SELECT
        id_venda,
        unidade,
        DATE(data_atendimento) AS data_atendimento,
        nome_cliente,
        profissional,
        forma_pagamento,
        nome_servico_simplificado,
        SUM(valor_liquido) AS valor_liquido,
        SUM(valor_bruto) AS valor_bruto,
        COUNT(*) AS qtd_itens
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE 
        data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
        AND tipo_item = 'Serviço'
        {filtro_unidade}
    GROUP BY id_venda, unidade, data_atendimento, nome_cliente, profissional, forma_pagamento, nome_servico_simplificado
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_atendimentos_detalhados(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()

    filtro_unidade = ""
    if unidade_filtro and unidade_filtro != 'TODAS':
        filtro_unidade = f"AND LOWER(unidade) = LOWER('{unidade_filtro}')"

    query = f"""
    SELECT
        id_venda,
        unidade,
        DATE(data_atendimento) AS data_atendimento,
        nome_cliente,
        profissional,
        forma_pagamento,
        nome_servico_simplificado,
        valor_liquido,
        valor_bruto
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE 
        data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
        AND tipo_item = 'Serviço'
        {filtro_unidade}
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
    LIMIT 200
    """
    return client.query(query).to_dataframe()['unidade'].tolist()

# -----------------------------------------------------------------------------
# FUNÇÕES DE DADOS – ECOMMERCE (CORRIGIDO)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_ecommerce_vendas(data_inicio, data_fim):
    """Carrega vouchers VENDIDOS (pela data de criação) - visão global"""
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
        (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    WHERE 
        s.CREATED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.CREATED_DATE < TIMESTAMP(DATE_ADD(DATE('{data_fim}'), INTERVAL 1 DAY), 'America/Sao_Paulo')
        AND s.STATUS IN ('1','2','3')
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_ecommerce_usados(data_inicio, data_fim):
    """Carrega vouchers USADOS (pela data de uso) - com unidade"""
    client = get_bigquery_client()
    query = f"""
    SELECT 
        s.ID,
        s.NAME,
        s.STATUS,
        s.CREATED_DATE,
        DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
        s.USED_DATE,
        DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL,
        s.PRICE_NET,
        s.PRICE_GROSS,
        s.KEY,
        s.ORDER_ID,
        (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
        (SELECT u.post_title 
           FROM `buddha-bigdata.raw.wp_posts` u 
          WHERE u.post_type = 'unidade' 
            AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
        ) AS AFILLIATION_NAME
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    WHERE 
        s.USED_DATE IS NOT NULL
        AND s.USED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.USED_DATE < TIMESTAMP(DATE_ADD(DATE('{data_fim}'), INTERVAL 1 DAY), 'America/Sao_Paulo')
        AND s.STATUS IN ('1','2','3')
    """
    return client.query(query).to_dataframe()

# -----------------------------------------------------------------------------
# FUNÇÕES DE DADOS – GA4 (CORREÇÃO FINAL DE TIPOS E DATA)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_ga4_pages(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT
      PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS data,
      pagePath AS page_path,
      pageTitle AS page_title,
      CAST(screenPageViews AS FLOAT64) AS page_views,
      CAST(totalUsers AS FLOAT64) AS usuarios,
      CAST(averageSessionDuration AS FLOAT64) AS duracao_media_sessao
    FROM `buddha-bigdata.ga4_historical_us.ga4_pages_historical`
    WHERE PARSE_DATE('%Y%m%d', CAST(date AS STRING)) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_ga4_traffic(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT
      PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS data,
      sessionDefaultChannelGrouping AS canal,
      sessionSource AS origem,
      sessionMedium AS meio,
      deviceCategory AS dispositivo,
      SUM(CAST(sessions AS FLOAT64)) AS sessoes,
      SUM(CAST(totalUsers AS FLOAT64)) AS usuarios,
      SUM(CAST(newUsers AS FLOAT64)) AS novos_usuarios,
      SUM(CAST(screenPageViews AS FLOAT64)) AS pageviews,
      SUM(CAST(userEngagementDuration AS FLOAT64)) AS duracao_engajamento
    FROM `buddha-bigdata.ga4_historical_us.ga4_traffic_sources_historical`
    WHERE PARSE_DATE('%Y%m%d', CAST(date AS STRING)) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    GROUP BY data, canal, origem, meio, dispositivo
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_ga4_events(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT
      PARSE_DATE('%Y%m%d', CAST(date AS STRING)) AS data,
      eventName AS evento,
      sessionDefaultChannelGrouping AS canal,
      SUM(CAST(eventCount AS FLOAT64)) AS total_eventos,
      SUM(CAST(totalUsers AS FLOAT64)) AS usuarios
    FROM `buddha-bigdata.ga4_historical_us.ga4_events_historical`
    WHERE PARSE_DATE('%Y%m%d', CAST(date AS STRING)) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    GROUP BY data, evento, canal
    """
    return client.query(query).to_dataframe()

# -----------------------------------------------------------------------------
# FUNÇÕES – INSTAGRAM / META ADS
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_instagram_posts(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT
      DATE(data) AS data_post,
      nome,
      visualizacoes,
      compartilhamentos,
      curtidas,
      comentarios,
      impressoes,
      alcance,
      vendas,
      id_post
    FROM `buddha-bigdata.raw.instagram_posts`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_instagram_seguidores(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT
      DATE(data) AS data_registro,
      qtd_seguidores
    FROM `buddha-bigdata.raw.instagram_seguidores`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    ORDER BY data_registro
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_meta_ads(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT
      DATE(data) AS data,
      nome,
      impressoes,
      alcance,
      cliques,
      vendas,
      investido,
      vendas_valor
    FROM `buddha-bigdata.raw.meta_ads`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    """
    return client.query(query).to_dataframe()

# -----------------------------------------------------------------------------
# SIDEBAR – FILTROS
# -----------------------------------------------------------------------------
st.sidebar.title("Filtros")

st.sidebar.success(f"Bem-vindo, {st.session_state.nome}!")
if st.sidebar.button("Sair", use_container_width=True):
    fazer_logout()
    st.rerun()

st.sidebar.markdown("---")

col1, col2 = st.sidebar.columns(2)
data_inicio = col1.date_input("De:", value=datetime(2024, 1, 1))
data_fim = col2.date_input("Até:", value=datetime.now())

if is_admin:
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
else:
    unidades_selecionadas = [unidade_usuario.lower()]
    st.sidebar.info(f"Você está visualizando apenas: **{unidade_usuario}**")

# -----------------------------------------------------------------------------
# CARREGAR DADOS PRINCIPAIS
# -----------------------------------------------------------------------------
with st.spinner("Carregando dados de atendimentos..."):
    try:
        if is_admin and not unidades_selecionadas:
            df = load_atendimentos(data_inicio, data_fim, unidade_filtro=None)
            df_detalhado = load_atendimentos_detalhados(data_inicio, data_fim, unidade_filtro=None)
        elif is_admin and unidades_selecionadas:
            df = load_atendimentos(data_inicio, data_fim, unidade_filtro=None)
            df = df[df['unidade'].str.lower().isin(unidades_selecionadas)]
            df_detalhado = load_atendimentos_detalhados(data_inicio, data_fim, unidade_filtro=None)
            df_detalhado = df_detalhado[df_detalhado['unidade'].str.lower().isin(unidades_selecionadas)]
        else:
            df = load_atendimentos(data_inicio, data_fim, unidade_filtro=unidade_usuario)
            df_detalhado = load_atendimentos_detalhados(data_inicio, data_fim, unidade_filtro=unidade_usuario)
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

if df.empty:
    st.warning("Sem dados de atendimentos para o período/unidades selecionados.")
    st.stop()

data_col = 'data_atendimento'
valor_col = 'valor_liquido'

# -----------------------------------------------------------------------------
# HEADER / KPIs
# -----------------------------------------------------------------------------
st.title("Buddha Spa - Dashboard de Unidades")
st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

receita_total = df[valor_col].sum()
qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0
ticket_medio = receita_total / qtd_atendimentos if qtd_atendimentos > 0 else 0

colk1, colk2, colk3, colk4 = st.columns(4)
colk1.metric("Receita Total (Presencial)", f"R$ {receita_total:,.2f}")
colk2.metric("Quantidade de Atendimentos", f"{qtd_atendimentos:,d}")
colk3.metric("Clientes Únicos", f"{qtd_clientes:,d}")
colk4.metric("Ticket Médio por Atendimento", f"R$ {ticket_medio:,.2f}")

st.divider()

# -----------------------------------------------------------------------------
# TABS
# -----------------------------------------------------------------------------
tab_visao, tab_atend, tab_fin, tab_mkt, tab_selfservice, tab_gloss = st.tabs(
    ["Visão Geral", "Atendimento", "Financeiro", "Marketing & Ecommerce", "Self-Service", "Ajuda / Glossário"]
)

# ---------------------- TAB: VISÃO GERAL -------------------------
with tab_visao:
    st.subheader("Evolução da Receita (Presencial)")

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
    st.subheader("Receita por Unidade (Presencial)")

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

    if 'profissional' in df.columns:
        df_terap = (
            df.groupby(['unidade', 'profissional'])
            .agg(
                receita=(valor_col, 'sum'),
                qtd_atendimentos=('id_venda', 'nunique'),
                clientes_unicos=('nome_cliente', 'nunique') if 'nome_cliente' in df.columns else ('unidade', 'size')
            )
            .reset_index()
        )
        df_terap['ticket_medio'] = df_terap['receita'] / df_terap['qtd_atendimentos']
        df_terap = df_terap.sort_values('receita', ascending=False)

        cola, colb = st.columns([2, 1])

        with cola:
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

        with colb:
            st.markdown("### Tabela de Performance")
            st.dataframe(
                df_terap.style.format({
                    'receita': 'R$ {:,.2f}',
                    'qtd_atendimentos': '{:,.0f}',
                    'clientes_unicos': '{:,.0f}',
                    'ticket_medio': 'R$ {:,.2f}'
                }),
                use_container_width=True,
                height=500
            )

    st.markdown("---")
    st.subheader("Principais Serviços (Presencial)")

    if 'nome_servico_simplificado' in df_detalhado.columns:
        df_servicos = (
            df_detalhado.groupby('nome_servico_simplificado')[valor_col]
            .agg(['sum', 'count'])
            .reset_index()
            .rename(columns={'sum': 'receita', 'count': 'qtd'})
        )
        df_servicos['perc_receita'] = df_servicos['receita'] / df_servicos['receita'].sum()
        df_servicos = df_servicos.sort_values('receita', ascending=False).head(15)

        cols1, cols2 = st.columns([2, 1])

        with cols1:
            fig_s = px.bar(
                df_servicos,
                x='receita',
                y='nome_servico_simplificado',
                orientation='h',
                text=df_servicos['perc_receita'].map(lambda x: f"{x*100:.1f}%"),
                labels={'receita': 'Receita (R$)', 'nome_servico_simplificado': 'Serviço'}
            )
            fig_s.update_traces(marker_color='#8B0000', textposition='outside')
            fig_s.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=500
            )
            st.plotly_chart(fig_s, use_container_width=True)

        with cols2:
            st.dataframe(
                df_servicos.rename(columns={
                    'nome_servico_simplificado': 'Serviço',
                    'receita': 'Receita',
                    'qtd': 'Quantidade',
                    'perc_receita': '% Receita'
                }).style.format({
                    'Receita': 'R$ {:,.2f}',
                    'Quantidade': '{:,.0f}',
                    '% Receita': '{:.1%}'
                }),
                use_container_width=True,
                height=500
            )

# ---------------------- TAB: FINANCEIRO -------------------------
with tab_fin:
    st.subheader("Resumo Financeiro da Unidade")

    colf1, colf2, colf3 = st.columns(3)
    colf1.metric("Receita Total (Atendimentos Presenciais)", f"R$ {receita_total:,.2f}")
    colf2.metric("Quantidade de Atendimentos", f"{qtd_atendimentos:,d}")
    colf3.metric("Ticket Médio Unidade", f"R$ {ticket_medio:,.2f}")

    st.markdown("---")
    st.subheader("Receita por Unidade (Presencial)")

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
    st.subheader("Serviços Presenciais Mais Vendidos (Financeiro)")

    if 'nome_servico_simplificado' in df_detalhado.columns:
        df_serv_fin = (
            df_detalhado.groupby('nome_servico_simplificado')[valor_col]
            .agg(['sum', 'count'])
            .reset_index()
            .rename(columns={'sum': 'receita', 'count': 'qtd'})
        )
        df_serv_fin = df_serv_fin.sort_values('receita', ascending=False).head(10)

        colf_s1, colf_s2 = st.columns([2, 1])

        with colf_s1:
            fig_sf = px.bar(
                df_serv_fin,
                x='receita',
                y='nome_servico_simplificado',
                orientation='h',
                labels={'receita': 'Receita (R$)', 'nome_servico_simplificado': 'Serviço'}
            )
            fig_sf.update_traces(marker_color='#8B0000')
            fig_sf.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400
            )
            st.plotly_chart(fig_sf, use_container_width=True)

        with colf_s2:
            st.dataframe(
                df_serv_fin.rename(columns={
                    'nome_servico_simplificado': 'Serviço',
                    'receita': 'Receita',
                    'qtd': 'Quantidade'
                }).style.format({
                    'Receita': 'R$ {:,.2f}',
                    'Quantidade': '{:,.0f}'
                }),
                use_container_width=True,
                height=400
            )

    st.markdown("---")
    st.subheader("Vouchers Usados por Unidade (Ecommerce)")

    with st.spinner("Carregando vouchers usados..."):
        try:
            df_ecom_usados = load_ecommerce_usados(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar vouchers usados: {e}")
            df_ecom_usados = pd.DataFrame()

    if df_ecom_usados.empty:
        st.info("Sem vouchers usados no período selecionado.")
    else:
        df_ecom_usados['PRICE_NET'] = pd.to_numeric(df_ecom_usados['PRICE_NET'], errors='coerce').fillna(0)
        df_ecom_usados['AFILLIATION_NAME'] = df_ecom_usados['AFILLIATION_NAME'].fillna('Sem Unidade')

        df_ecom_unid = (
            df_ecom_usados
            .groupby('AFILLIATION_NAME')
            .agg(
                qtde_vouchers=('ID', 'count'),
                receita_liquida=('PRICE_NET', 'sum')
            )
            .reset_index()
            .sort_values('receita_liquida', ascending=False)
        )

        colf_e1, colf_e2 = st.columns([2, 1])

        with colf_e1:
            fig_eu = px.bar(
                df_ecom_unid,
                x='receita_liquida',
                y='AFILLIATION_NAME',
                orientation='h',
                labels={'receita_liquida': 'Receita Líquida (R$)', 'AFILLIATION_NAME': 'Unidade'}
            )
            fig_eu.update_traces(marker_color='#A52A2A')
            fig_eu.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400
            )
            st.plotly_chart(fig_eu, use_container_width=True)

        with colf_e2:
            st.dataframe(
                df_ecom_unid.rename(columns={
                    'AFILLIATION_NAME': 'Unidade',
                    'qtde_vouchers': 'Qtd Vouchers Usados',
                    'receita_liquida': 'Receita Líquida'
                }).style.format({
                    'Qtd Vouchers Usados': '{:,.0f}',
                    'Receita Líquida': 'R$ {:,.2f}'
                }),
                use_container_width=True,
                height=400
            )

        st.info("⚠️ **Atenção:** Esta receita refere-se apenas aos vouchers que **já foram usados** nas unidades. Vouchers vendidos mas ainda não utilizados não aparecem aqui.")

# ---------------------- TAB: MARKETING & ECOMMERCE -------------------------
with tab_mkt:
    # BLOCO 1 – ECOMMERCE (VENDAS GLOBAIS)
    st.subheader("Ecommerce – Vendas de Vouchers (Visão Global)")

    with st.spinner("Carregando dados de ecommerce..."):
        try:
            df_ecom_vendas = load_ecommerce_vendas(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar dados de ecommerce: {e}")
            df_ecom_vendas = pd.DataFrame()

    if df_ecom_vendas.empty:
        st.warning("Sem dados de ecommerce para o período selecionado.")
    else:
        df_ecom_vendas['PRICE_GROSS'] = pd.to_numeric(df_ecom_vendas['PRICE_GROSS'], errors='coerce').fillna(0)
        df_ecom_vendas['PRICE_NET'] = pd.to_numeric(df_ecom_vendas['PRICE_NET'], errors='coerce').fillna(0)

        if 'PACKAGE_NAME' in df_ecom_vendas.columns:
            df_ecom_vendas['PACKAGE_NAME'] = df_ecom_vendas['PACKAGE_NAME'].fillna(df_ecom_vendas['NAME'])
        else:
            df_ecom_vendas['PACKAGE_NAME'] = df_ecom_vendas['NAME']

        colm1, colm2, colm3, colm4 = st.columns(4)

        total_pedidos = int(df_ecom_vendas['ORDER_ID'].nunique())
        total_vouchers = int(len(df_ecom_vendas))
        receita_liquida_e = df_ecom_vendas['PRICE_NET'].sum()
        ticket_medio_e = receita_liquida_e / total_pedidos if total_pedidos > 0 else 0

        colm1.metric("Pedidos Ecommerce", f"{total_pedidos:,.0f}")
        colm2.metric("Vouchers Vendidos", f"{total_vouchers:,.0f}")
        colm3.metric("Receita Líquida Ecommerce", f"R$ {receita_liquida_e:,.2f}")
        colm4.metric("Ticket Médio Ecommerce", f"R$ {ticket_medio_e:,.2f}")

        st.markdown("### Top 10 Serviços / Pacotes Vendidos (Ecommerce)")

        df_serv = (
            df_ecom_vendas
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

        st.info("💡 **Nota:** Estes números representam vouchers **vendidos** (pela data de criação), independente de terem sido usados ou não.")

    st.markdown("---")

    # BLOCO 2 – SITE / GA4 PÁGINAS
    st.subheader("Site – Pageviews por Página (GA4)")

    with st.spinner("Carregando dados de páginas GA4..."):
        try:
            df_ga4_pages = load_ga4_pages(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar GA4 páginas: {e}")
            df_ga4_pages = pd.DataFrame()

    if df_ga4_pages.empty:
        st.warning("Sem dados de páginas GA4 para o período selecionado.")
    else:
        df_ga4_pages['tipo_pagina'] = 'Outras'
        df_ga4_pages.loc[df_ga4_pages['page_path'].str.contains('franquia', case=False, na=False), 'tipo_pagina'] = 'Franquias'
        df_ga4_pages.loc[df_ga4_pages['page_path'].str.contains('voucher', case=False, na=False), 'tipo_pagina'] = 'Ecommerce'
        df_ga4_pages.loc[df_ga4_pages['page_path'].str.contains('curso', case=False, na=False), 'tipo_pagina'] = 'Cursos'

        colg1, colg2, colg3 = st.columns(3)

        total_pageviews = int(df_ga4_pages['page_views'].sum())
        total_usuarios = int(df_ga4_pages['usuarios'].sum())
        duracao_media = df_ga4_pages['duracao_media_sessao'].mean() if not df_ga4_pages['duracao_media_sessao'].isna().all() else 0

        colg1.metric("Pageviews Totais (Site)", f"{total_pageviews:,d}")
        colg2.metric("Usuários Totais", f"{total_usuarios:,d}")
        colg3.metric("Duração Média da Sessão (s)", f"{duracao_media:,.1f}")

        st.markdown("### Pageviews por Tipo de Página")
        df_tipo = (
            df_ga4_pages.groupby('tipo_pagina')['page_views']
            .sum()
            .reset_index()
            .sort_values('page_views', ascending=False)
        )

        fig_pag = px.bar(
            df_tipo,
            x='page_views',
            y='tipo_pagina',
            orientation='h',
            labels={'page_views': 'Pageviews', 'tipo_pagina': 'Tipo de Página'}
        )
        fig_pag.update_traces(marker_color='#8B0000')
        fig_pag.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400
        )
        st.plotly_chart(fig_pag, use_container_width=True)

    st.markdown("---")

    # BLOCO 3 – SITE / GA4 TRÁFEGO
    st.subheader("Site – Canais de Aquisição (GA4)")

    with st.spinner("Carregando dados de tráfego GA4..."):
        try:
            df_ga4_traffic = load_ga4_traffic(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar GA4 tráfego: {e}")
            df_ga4_traffic = pd.DataFrame()

    if df_ga4_traffic.empty:
        st.warning("Sem dados de tráfego GA4 para o período selecionado.")
    else:
        colt1, colt2, colt3 = st.columns(3)

        total_sessoes = int(df_ga4_traffic['sessoes'].sum())
        total_usuarios_t = int(df_ga4_traffic['usuarios'].sum())
        total_novos = int(df_ga4_traffic['novos_usuarios'].sum())

        colt1.metric("Sessões Totais", f"{total_sessoes:,d}")
        colt2.metric("Usuários Totais", f"{total_usuarios_t:,d}")
        colt3.metric("Novos Usuários", f"{total_novos:,d}")

        st.markdown("### Sessões por Canal")
        df_canal = (
            df_ga4_traffic.groupby('canal')['sessoes']
            .sum()
            .reset_index()
            .sort_values('sessoes', ascending=False)
        )

        fig_can = px.bar(
            df_canal,
            x='sessoes',
            y='canal',
            orientation='h',
            labels={'canal': 'Canal', 'sessoes': 'Sessões'}
        )
        fig_can.update_traces(marker_color='#A52A2A')
        fig_can.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400
        )
        st.plotly_chart(fig_can, use_container_width=True)

        st.markdown("### Sessões por Dispositivo")
        df_disp = (
            df_ga4_traffic.groupby('dispositivo')['sessoes']
            .sum()
            .reset_index()
            .sort_values('sessoes', ascending=False)
        )

        fig_disp = px.pie(
            df_disp,
            names='dispositivo',
            values='sessoes',
            labels={'dispositivo': 'Dispositivo', 'sessoes': 'Sessões'}
        )
        fig_disp.update_layout(
            paper_bgcolor='#F5F0E6',
            height=400
        )
        st.plotly_chart(fig_disp, use_container_width=True)

    st.markdown("---")

    # BLOCO 4 – EVENTOS GA4
    st.subheader("Site – Eventos Principais (GA4)")

    with st.spinner("Carregando eventos GA4..."):
        try:
            df_ga4_events = load_ga4_events(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar eventos GA4: {e}")
            df_ga4_events = pd.DataFrame()

    if df_ga4_events.empty:
        st.warning("Sem dados de eventos GA4 para o período selecionado.")
    else:
        eventos_interesse = ['form_submit', 'form_start', 'click', 'RD Popup e WhatsApp', 'RD Landing Pages']
        df_eventos_filtro = df_ga4_events[df_ga4_events['evento'].isin(eventos_interesse)]

        if df_eventos_filtro.empty:
            st.info("Nenhum evento de interesse (form_submit, click, etc.) encontrado no período.")
        else:
            df_eventos_agg = (
                df_eventos_filtro.groupby('evento')['total_eventos']
                .sum()
                .reset_index()
                .sort_values('total_eventos', ascending=False)
            )

            fig_ev = px.bar(
                df_eventos_agg,
                x='total_eventos',
                y='evento',
                orientation='h',
                labels={'total_eventos': 'Total de Eventos', 'evento': 'Evento'}
            )
            fig_ev.update_traces(marker_color='#8B0000')
            fig_ev.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400
            )
            st.plotly_chart(fig_ev, use_container_width=True)

    st.markdown("---")

    # BLOCO 5 – INSTAGRAM POSTS
    st.subheader("Redes Sociais – Posts com Melhor Performance (Instagram)")

    with st.spinner("Carregando posts do Instagram..."):
        try:
            df_ig = load_instagram_posts(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar instagram_posts: {e}")
            df_ig = pd.DataFrame()

    if df_ig.empty:
        st.info("Sem posts do Instagram para o período selecionado.")
    else:
        for col in ['visualizacoes', 'compartilhamentos', 'curtidas', 'comentarios', 'impressoes', 'alcance', 'vendas']:
            if col in df_ig.columns:
                df_ig[col] = pd.to_numeric(df_ig[col], errors='coerce').fillna(0)

        coli1, coli2, coli3, coli4 = st.columns(4)
        total_posts = len(df_ig)
        total_curtidas = int(df_ig['curtidas'].sum())
        total_coment = int(df_ig['comentarios'].sum())
        total_impressoes = int(df_ig['impressoes'].sum()) if 'impressoes' in df_ig.columns else 0

        coli1.metric("Total de Posts", f"{total_posts:,d}")
        coli2.metric("Total de Curtidas", f"{total_curtidas:,d}")
        coli3.metric("Total de Comentários", f"{total_coment:,d}")
        coli4.metric("Total de Impressões", f"{total_impressoes:,d}")

        st.markdown("### Top 10 Posts por Engajamento (Curtidas + Comentários)")
        df_ig['engajamento'] = df_ig['curtidas'] + df_ig['comentarios']
        df_top_eng = (
            df_ig.sort_values('engajamento', ascending=False)
            .head(10)
            .copy()
        )
        df_top_eng['legenda_curta'] = df_top_eng['nome'].str.slice(0, 60) + "..."

        colg_a, colg_b = st.columns([2, 1])

        with colg_a:
            fig_ig = px.bar(
                df_top_eng,
                x='engajamento',
                y='legenda_curta',
                orientation='h',
                labels={'engajamento': 'Engajamento (Curtidas + Comentários)', 'legenda_curta': 'Post'},
                text='engajamento'
            )
            fig_ig.update_traces(marker_color='#8B0000', textposition='outside')
            fig_ig.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=500
            )
            st.plotly_chart(fig_ig, use_container_width=True)

        with colg_b:
            st.markdown("#### Detalhes dos Top Posts")
            st.dataframe(
                df_top_eng[[
                    'data_post', 'nome', 'visualizacoes', 'curtidas', 'comentarios', 'compartilhamentos', 'alcance'
                ]].rename(columns={
                    'data_post': 'Data',
                    'nome': 'Legenda',
                    'visualizacoes': 'Visualizações',
                    'curtidas': 'Curtidas',
                    'comentarios': 'Comentários',
                    'compartilhamentos': 'Compart.',
                    'alcance': 'Alcance'
                }),
                use_container_width=True,
                height=500
            )

    st.markdown("---")

    # BLOCO 6 – SEGUIDORES INSTAGRAM
    st.subheader("Redes Sociais – Crescimento de Seguidores (Instagram)")

    with st.spinner("Carregando dados de seguidores..."):
        try:
            df_seg = load_instagram_seguidores(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar instagram_seguidores: {e}")
            df_seg = pd.DataFrame()

    if df_seg.empty:
        st.info("Sem dados de seguidores para o período selecionado.")
    else:
        df_seg['qtd_seguidores'] = pd.to_numeric(df_seg['qtd_seguidores'], errors='coerce')
        df_seg = df_seg.sort_values('data_registro')

        seg_inicio = int(df_seg.iloc[0]['qtd_seguidores']) if len(df_seg) > 0 else 0
        seg_fim = int(df_seg.iloc[-1]['qtd_seguidores']) if len(df_seg) > 0 else 0
        crescimento = seg_fim - seg_inicio
        perc_crescimento = (crescimento / seg_inicio * 100) if seg_inicio > 0 else 0

        cols1, cols2, cols3 = st.columns(3)
        cols1.metric("Seguidores Atuais", f"{seg_fim:,d}")
        cols2.metric("Crescimento no Período", f"{crescimento:+,d}", delta=f"{perc_crescimento:+.2f}%")
        cols3.metric("Seguidores no Início", f"{seg_inicio:,d}")

        st.markdown("### Evolução de Seguidores")
        fig_seg = px.line(
            df_seg,
            x='data_registro',
            y='qtd_seguidores',
            markers=True,
            labels={'data_registro': 'Data', 'qtd_seguidores': 'Seguidores'}
        )
        fig_seg.update_traces(line_color='#8B0000', marker=dict(color='#8B0000'))
        fig_seg.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400
        )
        st.plotly_chart(fig_seg, use_container_width=True)

    st.markdown("---")

    # BLOCO 7 – META ADS
    st.subheader("Mídia Paga – Meta Ads")

    with st.spinner("Carregando dados de Meta Ads..."):
        try:
            df_meta = load_meta_ads(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar meta_ads: {e}")
            df_meta = pd.DataFrame()

    if df_meta.empty:
        st.info("Sem dados de Meta Ads para o período selecionado.")
    else:
        for col in ['impressoes', 'alcance', 'cliques', 'vendas', 'investido', 'vendas_valor']:
            if col in df_meta.columns:
                df_meta[col] = pd.to_numeric(df_meta[col], errors='coerce').fillna(0)

        m1, m2, m3, m4 = st.columns(4)
        total_imp = int(df_meta['impressoes'].sum())
        total_clicks = int(df_meta['cliques'].sum())
        total_invest = df_meta['investido'].sum()
        total_vendas_valor = df_meta['vendas_valor'].sum()

        ctr = (total_clicks / total_imp * 100) if total_imp > 0 else 0
        roi = ((total_vendas_valor - total_invest) / total_invest * 100) if total_invest > 0 else 0

        m1.metric("Impressões", f"{total_imp:,d}")
        m2.metric("Cliques", f"{total_clicks:,d}", delta=f"CTR {ctr:.2f}%")
        m3.metric("Investimento (R$)", f"R$ {total_invest:,.2f}")
        m4.metric("Receita Atribuída (R$)", f"R$ {total_vendas_valor:,.2f}", delta=f"ROI {roi:.1f}%")

        st.markdown("### Campanhas com Maior Investimento")

        df_meta_camp = (
            df_meta
            .groupby('nome')
            .agg(
                impressoes=('impressoes', 'sum'),
                cliques=('cliques', 'sum'),
                vendas=('vendas', 'sum'),
                investido=('investido', 'sum'),
                vendas_valor=('vendas_valor', 'sum')
            )
            .reset_index()
        )
        df_meta_camp['ctr'] = df_meta_camp['cliques'] / df_meta_camp['impressoes']
        df_meta_camp['cpc'] = df_meta_camp['investido'] / df_meta_camp['cliques']
        df_meta_camp['roi'] = (df_meta_camp['vendas_valor'] - df_meta_camp['investido']) / df_meta_camp['investido']

        df_meta_top = df_meta_camp.sort_values('investido', ascending=False).head(10)

        fig_meta = px.bar(
            df_meta_top,
            x='investido',
            y='nome',
            orientation='h',
            labels={'investido': 'Investimento (R$)', 'nome': 'Campanha'}
        )
        fig_meta.update_traces(marker_color='#8B0000')
        fig_meta.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=450
        )
        st.plotly_chart(fig_meta, use_container_width=True)

        st.markdown("#### Detalhes das Campanhas (Top 10 por Investimento)")
        st.dataframe(
            df_meta_top.rename(columns={
                'nome': 'Campanha',
                'impressoes': 'Impressões',
                'cliques': 'Cliques',
                'vendas': 'Vendas',
                'investido': 'Investido (R$)',
                'vendas_valor': 'Receita (R$)',
                'ctr': 'CTR',
                'cpc': 'CPC',
                'roi': 'ROI'
            }).style.format({
                'Impressões': '{:,.0f}',
                'Cliques': '{:,.0f}',
                'Vendas': '{:,.0f}',
                'Investido (R$)': 'R$ {:,.2f}',
                'Receita (R$)': 'R$ {:,.2f}',
                'CTR': '{:.2%}',
                'CPC': 'R$ {:,.2f}',
                'ROI': '{:.1%}'
            }),
            use_container_width=True,
            height=450
        )

# ---------------------- TAB: SELF-SERVICE -------------------------
with tab_selfservice:
    st.subheader("Monte Sua Própria Análise")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("### Agrupar Por")
        dimensoes = st.multiselect(
            "Selecione dimensões:",
            ["Data", "Unidade", "Forma de Pagamento", "Serviço", "Terapeuta", "Cliente"],
            default=["Unidade"]
        )

    with c2:
        st.markdown("### Métricas")
        metricas = st.multiselect(
            "Selecione métricas:",
            ["Receita Total", "Quantidade de Atendimentos", "Ticket Médio", "Clientes Únicos"],
            default=["Receita Total", "Quantidade de Atendimentos"]
        )

    if dimensoes and metricas:
        dim_map = {
            "Data": data_col,
            "Unidade": "unidade",
            "Forma de Pagamento": "forma_pagamento",
            "Serviço": "nome_servico_simplificado",
            "Terapeuta": "profissional",
            "Cliente": "nome_cliente"
        }

        colunas_agrupamento = [dim_map[d] for d in dimensoes if dim_map[d] in df.columns]

        if colunas_agrupamento:
            agg_dict = {}
            if "Receita Total" in metricas:
                agg_dict['receita_total'] = (valor_col, 'sum')
            if "Quantidade de Atendimentos" in metricas:
                agg_dict['qtd_atendimentos'] = ('id_venda', 'nunique')
            if "Clientes Únicos" in metricas and 'nome_cliente' in df.columns:
                agg_dict['clientes_unicos'] = ('nome_cliente', 'nunique')

            df_custom = df.groupby(colunas_agrupamento).agg(**agg_dict).reset_index()

            if "Ticket Médio" in metricas and 'receita_total' in df_custom.columns and 'qtd_atendimentos' in df_custom.columns:
                df_custom['ticket_medio'] = df_custom['receita_total'] / df_custom['qtd_atendimentos']

            st.markdown("---")
            st.dataframe(df_custom, use_container_width=True, height=400)

            csv = df_custom.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Download CSV",
                csv,
                f"buddha_selfservice_{data_inicio}_{data_fim}.csv",
                "text/csv"
            )

# ---------------------- TAB: AJUDA / GLOSSÁRIO -------------------------
with tab_gloss:
    st.subheader("Ajuda / Glossário de Métricas")

    st.markdown("""
    ### 📊 Principais Métricas

    **Receita Total (Presencial)** – Soma de todos os valores líquidos de atendimentos presenciais no período.  
    **Quantidade de Atendimentos** – Número de atendimentos únicos (`id_venda`).  
    **Clientes Únicos** – Número de clientes distintos atendidos.  
    **Ticket Médio por Atendimento** – Receita Total ÷ Quantidade de Atendimentos.  

    **Serviços Presenciais Mais Vendidos** – Ranking de serviços presenciais por receita e quantidade.  
    
    ---
    
    ### 🛒 Ecommerce
    
    **Pedidos Ecommerce** – Número de pedidos únicos realizados no site (pela data de criação).  
    **Vouchers Vendidos** – Total de vouchers vendidos (independente de terem sido usados).  
    **Receita Líquida Ecommerce** – Soma do valor líquido de todos os vouchers vendidos.  
    **Vouchers Usados por Unidade** – Receita e quantidade de vouchers que **já foram utilizados** em cada unidade (pela data de uso).  
    
    ⚠️ **Importante:** A receita de ecommerce global representa vouchers **vendidos**. A receita por unidade representa apenas vouchers **já usados** nas unidades.
    
    ---

    ### 🌐 Site (GA4)
    
    **Pageviews (GA4)** – Visualizações de página no site / páginas-chave.  
    **Sessões (GA4)** – Sessões por canal de aquisição (Direct, Organic, Paid, Social etc.).  
    **Eventos (GA4)** – Eventos como `form_submit`, cliques, WhatsApp etc.  

    ---
    
    ### 📱 Redes Sociais
    
    **Seguidores Instagram** – Evolução de `qtd_seguidores` ao longo do tempo.  
    **Engajamento** – Soma de curtidas + comentários dos posts.  
    
    ---
    
    ### 💰 Mídia Paga
    
    **Meta Ads** – Impressões, cliques, investimento, vendas e ROI das campanhas do Facebook/Instagram.  
    **CTR (Click-Through Rate)** – Taxa de cliques = (Cliques ÷ Impressões) × 100.  
    **CPC (Custo Por Clique)** – Investimento ÷ Cliques.  
    **ROI (Return on Investment)** – ((Receita - Investimento) ÷ Investimento) × 100.
    """)

st.caption("Buddha Spa Dashboard – Portal de Franqueados | Dados desde 2024")
