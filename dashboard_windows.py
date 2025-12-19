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
        'unidade': 'buddha spa - higienópolis'   # tem que bater com o texto da coluna unidade
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
# FUNÇÕES DE DADOS
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
    """Item a item – para serviços mais vendidos, etc."""
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
           AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)) AS AFILLIATION_NAME
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    WHERE 
        s.CREATED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.CREATED_DATE <= TIMESTAMP('{data_fim} 23:59:59', 'America/Sao_Paulo')
        AND s.STATUS IN ('1','2','3')
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_ga4_pages(data_inicio, data_fim):
    """Exemplo – ajuste os nomes de colunas se na sua tabela forem diferentes."""
    client = get_bigquery_client()
    query = f"""
    SELECT
      DATE(event_date) AS data,
      page_path,
      page_title,
      device_category,
      session_default_channel_group AS canal,
      SUM(sessions) AS sessoes,
      SUM(total_users) AS usuarios,
      SUM(new_users) AS novos_usuarios
    FROM `buddha-bigdata.analytics.ga4_pages_historical`
    WHERE event_date BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    GROUP BY data, page_path, page_title, device_category, canal
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
data_inicio = col1.date_input("De:", value=datetime(2025, 1, 1))
data_fim = col2.date_input("Até:", value=datetime(2025, 9, 30))

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
# CARREGAR DADOS
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
# HEADER
# -----------------------------------------------------------------------------
st.title("Buddha Spa - Dashboard de Unidades")
st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

# -----------------------------------------------------------------------------
# KPIs PRINCIPAIS
# -----------------------------------------------------------------------------
receita_total = df[valor_col].sum()
qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0
ticket_medio = receita_total / qtd_atendimentos if qtd_atendimentos > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Receita Total", f"R$ {receita_total:,.2f}")
col2.metric("Quantidade de Atendimentos", f"{qtd_atendimentos:,d}")
col3.metric("Clientes Únicos", f"{qtd_clientes:,d}")
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
        df_servicos = df_servicos.sort_values('receita', ascending=False).take(range(0, min(15, len(df_servicos))))

        col1_s, col2_s = st.columns([2, 1])

        with col1_s:
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

        with col2_s:
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
    colf1.metric("Receita Total (Atendimentos)", f"R$ {receita_total:,.2f}")
    colf2.metric("Quantidade de Atendimentos", f"{qtd_atendimentos:,d}")
    colf3.metric("Ticket Médio Unidade", f"R$ {ticket_medio:,.2f}")

    st.markdown("---")
    st.subheader("Receita por Unidade")

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
    st.subheader("Itens Ecommerce Mais Vendidos (Financeiro)")

    with st.spinner("Carregando dados de ecommerce..."):
        try:
            df_ecom_fin = load_ecommerce_data(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar ecommerce para o financeiro: {e}")
            df_ecom_fin = pd.DataFrame()

    if df_ecom_fin.empty:
        st.info("Sem dados de ecommerce para o período selecionado.")
    else:
        df_ecom_fin['PRICE_NET'] = pd.to_numeric(df_ecom_fin['PRICE_NET'], errors='coerce')
        if 'PACKAGE_NAME' in df_ecom_fin.columns:
            df_ecom_fin['PACKAGE_NAME'] = df_ecom_fin['PACKAGE_NAME'].fillna(df_ecom_fin['NAME'])
        else:
            df_ecom_fin['PACKAGE_NAME'] = df_ecom_fin['NAME']

        df_ecom_top = (
            df_ecom_fin
            .groupby('PACKAGE_NAME')
            .agg(
                qtde_vouchers=('ID', 'count'),
                receita_liquida=('PRICE_NET', 'sum')
            )
            .reset_index()
            .sort_values('receita_liquida', ascending=False)
            .head(10)
        )

        colf_e1, colf_e2 = st.columns([2, 1])

        with colf_e1:
            fig_ef = px.bar(
                df_ecom_top,
                x='receita_liquida',
                y='PACKAGE_NAME',
                orientation='h',
                labels={'receita_liquida': 'Receita Líquida (R$)', 'PACKAGE_NAME': 'Serviço / Pacote'}
            )
            fig_ef.update_traces(marker_color='#A52A2A')
            fig_ef.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400
            )
            st.plotly_chart(fig_ef, use_container_width=True)

        with colf_e2:
            st.dataframe(
                df_ecom_top.rename(columns={
                    'PACKAGE_NAME': 'Serviço / Pacote',
                    'qtde_vouchers': 'Qtd Vouchers',
                    'receita_liquida': 'Receita Líquida'
                }).style.format({
                    'Qtd Vouchers': '{:,.0f}',
                    'Receita Líquida': 'R$ {:,.2f}'
                }),
                use_container_width=True,
                height=400
            )

# ---------------------- TAB: MARKETING & ECOMMERCE -------------------------
with tab_mkt:
    # BLOCO 1 – ECOMMERCE
    st.subheader("Ecommerce – Vendas de Vouchers")

    with st.spinner("Carregando dados de ecommerce..."):
        try:
            df_ecom = load_ecommerce_data(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar dados de ecommerce: {e}")
            df_ecom = pd.DataFrame()

    if df_ecom.empty:
        st.warning("Sem dados de ecommerce para o período selecionado.")
    else:
        df_ecom['PRICE_GROSS'] = pd.to_numeric(df_ecom['PRICE_GROSS'], errors='coerce')
        df_ecom['PRICE_NET'] = pd.to_numeric(df_ecom['PRICE_NET'], errors='coerce')

        if 'PACKAGE_NAME' in df_ecom.columns:
            df_ecom['PACKAGE_NAME'] = df_ecom['PACKAGE_NAME'].fillna(df_ecom['NAME'])
        else:
            df_ecom['PACKAGE_NAME'] = df_ecom['NAME']

        if 'AFILLIATION_NAME' not in df_ecom.columns:
            df_ecom['AFILLIATION_NAME'] = "Sem Unidade"

        colm1, colm2, colm3, colm4 = st.columns(4)

        total_pedidos = int(df_ecom['ORDER_ID'].nunique())
        total_vouchers = int(len(df_ecom))
        receita_liquida_e = df_ecom['PRICE_NET'].fillna(0).sum()
        ticket_medio_e = receita_liquida_e / total_pedidos if total_pedidos > 0 else 0

        colm1.metric("Pedidos Ecommerce", f"{total_pedidos:,.0f}")
        colm2.metric("Vouchers Vendidos", f"{total_vouchers:,.0f}")
        colm3.metric("Receita Líquida Ecommerce", f"R$ {receita_liquida_e:,.2f}")
        colm4.metric("Ticket Médio Ecommerce", f"R$ {ticket_medio_e:,.2f}")

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

    # BLOCO 2 – SITE / GA4
    st.subheader("Site – Sessões por Página (GA4)")

    with st.spinner("Carregando dados de GA4..."):
        try:
            df_ga4 = load_ga4_pages(data_inicio, data_fim)
        except Exception as e:
            st.error(f"Erro ao carregar GA4: {e}")
            df_ga4 = pd.DataFrame()

    if df_ga4.empty:
        st.warning("Sem dados de GA4 para o período selecionado.")
    else:
        df_ga4['tipo_pagina'] = 'Outras'
        df_ga4.loc[df_ga4['page_path'].str.contains('franquia', case=False, na=False), 'tipo_pagina'] = 'Franquias'
        df_ga4.loc[df_ga4['page_path'].str.contains('voucher', case=False, na=False), 'tipo_pagina'] = 'Ecommerce'
        df_ga4.loc[df_ga4['page_path'].str.contains('curso', case=False, na=False), 'tipo_pagina'] = 'Cursos'

        colg1, colg2, colg3 = st.columns(3)

        total_sessoes = int(df_ga4['sessoes'].sum())
        total_usuarios = int(df_ga4['usuarios'].sum())
        total_novos = int(df_ga4['novos_usuarios'].sum())

        colg1.metric("Sessões Totais (Site)", f"{total_sessoes:,d}")
        colg2.metric("Usuários Totais", f"{total_usuarios:,d}")
        colg3.metric("Novos Usuários", f"{total_novos:,d}")

        st.markdown("### Sessões por Tipo de Página")
        df_tipo = (
            df_ga4.groupby('tipo_pagina')['sessoes']
            .sum()
            .reset_index()
            .sort_values('sessoes', ascending=False)
        )

        fig_pag = px.bar(
            df_tipo,
            x='sessoes',
            y='tipo_pagina',
            orientation='h',
            labels={'sessoes': 'Sessões', 'tipo_pagina': 'Tipo de Página'}
        )
        fig_pag.update_traces(marker_color='#8B0000')
        fig_pag.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400
        )
        st.plotly_chart(fig_pag, use_container_width=True)

        st.markdown("### Canais de Aquisição (GA4)")
        df_canal = (
            df_ga4.groupby('canal')['sessoes']
            .sum()
            .reset_index()
            .sort_values('sessoes', ascending=False)
        )

        fig_can = px.bar(
            df_canal,
            x='canal',
            y='sessoes',
            labels={'canal': 'Canal', 'sessoes': 'Sessões'}
        )
        fig_can.update_traces(marker_color='#A52A2A')
        fig_can.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400
        )
        st.plotly_chart(fig_can, use_container_width=True)

    st.markdown("---")

    # BLOCO 3 – SEGUIDORES (placeholder – depende da tabela)
    st.subheader("Redes Sociais – Seguidores")
    st.info("Assim que você me passar o nome da tabela com seguidores no BigQuery, eu coloco aqui o gráfico de crescimento e ranking de perfis.")

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

    **Receita Total**  
    Soma de todos os valores líquidos de atendimentos no período selecionado.

    **Quantidade de Atendimentos**  
    Número de atendimentos únicos (id_venda) realizados no período.

    **Clientes Únicos**  
    Número de clientes distintos que foram atendidos.

    **Ticket Médio por Atendimento**  
    Receita Total ÷ Quantidade de Atendimentos.

    **Serviços Presenciais Mais Vendidos**  
    Ranking de serviços da tabela de atendimentos presenciais (itens_atendimentos_analytics), por receita e quantidade.

    **Itens Ecommerce Mais Vendidos**  
    Ranking de serviços/pacotes vendidos no ecommerce (vouchers), por receita líquida e quantidade de vouchers.

    **Sessões (Site / GA4)**  
    Número de sessões registradas pelo Google Analytics 4 no período.

    **Canais de Aquisição**  
    Origem do tráfego das sessões (Direct, Organic, Paid, Social, etc.), segundo GA4.
    """)

st.caption("Buddha Spa Dashboard – Portal de Franqueados")
