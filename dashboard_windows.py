import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime

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
# FUNÇÕES DE DADOS
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT 
        unidade,
        DATE(data_emissao) as data_emissao,
        forma_pagto,
        detalhamento_descricao as servico,
        detalhamento_vendedor as vendedor,
        cliente,
        valor_documento,
        valor_liquido,
        competencia
    FROM `buddha-bigdata.analytics.movimentacao_analitica`
    WHERE data_emissao BETWEEN '{data_inicio}' AND '{data_fim}'
    ORDER BY data_emissao DESC
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_unidades():
    client = get_bigquery_client()
    query = """
    SELECT DISTINCT LOWER(unidade) as unidade 
    FROM `buddha-bigdata.analytics.movimentacao_analitica`
    WHERE data_emissao >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    ORDER BY unidade
    LIMIT 50
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
# CARREGAR DADOS DE MOVIMENTAÇÃO
# -----------------------------------------------------------------------------
with st.spinner("Carregando dados de movimentação..."):
    try:
        df = load_data(data_inicio, data_fim)
        if unidades_selecionadas:
            df = df[df['unidade'].str.lower().isin(unidades_selecionadas)]
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        st.stop()

if df.empty:
    st.warning("Sem dados para o período/unidades selecionados.")
    st.stop()

# -----------------------------------------------------------------------------
# HEADER
# -----------------------------------------------------------------------------
st.title("Buddha Spa - Dashboard")
st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

# -----------------------------------------------------------------------------
# KPIs PRINCIPAIS
# -----------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

receita_total = df['valor_documento'].sum()
qtd_vendas = int(len(df))
ticket_medio = receita_total / qtd_vendas if qtd_vendas > 0 else 0
qtd_clientes = int(df['cliente'].nunique())

col1.metric("Receita Total", f"R$ {receita_total:,.2f}")
col2.metric("Quantidade de Vendas", f"{qtd_vendas:d}")
col3.metric("Ticket Médio", f"R$ {ticket_medio:,.2f}")
col4.metric("Clientes Únicos", f"{qtd_clientes:d}")

st.divider()

# -----------------------------------------------------------------------------
# TABS
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab_selfservice, tab_ecom = st.tabs(
    ["Evolução", "Unidades", "Top Serviços", "Self-Service", "Vendas Ecommerce"]
)

# ---------------------- TAB 1: EVOLUÇÃO -------------------------
with tab1:
    st.subheader("Receita Diária")
    df_evolucao = df.groupby('data_emissao')['valor_documento'].sum().reset_index()
    fig = px.line(df_evolucao, x='data_emissao', y='valor_documento', markers=True)
    fig.update_traces(line_color='#8B0000', marker=dict(color='#8B0000'))
    fig.update_layout(
        xaxis_title="Data", 
        yaxis_title="Receita (R$)", 
        height=400,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6'
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------------- TAB 2: UNIDADES -------------------------
with tab2:
    st.subheader("Receita por Unidade")
    df_unidades = df.groupby('unidade')['valor_documento'].sum().reset_index()
    df_unidades = df_unidades.sort_values('valor_documento', ascending=False)
    fig = px.bar(df_unidades, x='valor_documento', y='unidade', orientation='h')
    fig.update_traces(marker_color='#8B0000')
    fig.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6'
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------------- TAB 3: TOP SERVIÇOS ---------------------
with tab3:
    st.subheader("Top 10 Serviços")
    df_servicos = df.groupby('servico')['valor_documento'].sum().reset_index()
    df_servicos = df_servicos.sort_values('valor_documento', ascending=False).head(10)
    fig = px.bar(
        df_servicos,
        x='valor_documento',
        y='servico',
        orientation='h',
        color='valor_documento',
        color_continuous_scale=['#F08080', '#CD5C5C', '#B22222', '#A52A2A', '#8B0000']
    )
    fig.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6'
    )
    st.plotly_chart(fig, use_container_width=True)

# ---------------------- TAB 4: SELF-SERVICE ---------------------
with tab_selfservice:
    st.subheader("Monte Sua Própria Análise")
    st.caption("Escolha as dimensões e métricas - tipo QlikView!")
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("### Agrupar Por (Linhas)")
        dimensoes = st.multiselect(
            "Selecione uma ou mais dimensões:",
            ["Data", "Unidade", "Forma de Pagamento", "Serviço", "Vendedor"],
            default=["Unidade"],
            help="As linhas da sua tabela"
        )
    
    with c2:
        st.markdown("### Métricas (Colunas)")
        metricas = st.multiselect(
            "Selecione as métricas que deseja ver:",
            [
                "Receita Total",
                "Receita Líquida", 
                "Quantidade de Atendimentos",
                "Ticket Médio",
                "Clientes Únicos",
                "Maior Venda",
                "Menor Venda"
            ],
            default=["Receita Total", "Quantidade de Atendimentos", "Ticket Médio"],
            help="As colunas com os valores"
        )
    
    if not dimensoes:
        st.warning("Selecione pelo menos uma dimensão para agrupar")
    elif not metricas:
        st.warning("Selecione pelo menos uma métrica para exibir")
    else:
        dim_map = {
            "Data": "data_emissao",
            "Unidade": "unidade",
            "Forma de Pagamento": "forma_pagto",
            "Serviço": "servico",
            "Vendedor": "vendedor"
        }
        
        colunas_agrupamento = [dim_map[d] for d in dimensoes]
        
        agg_dict = {}
        if "Receita Total" in metricas:
            agg_dict['valor_documento_sum'] = ('valor_documento', 'sum')
        if "Receita Líquida" in metricas:
            agg_dict['valor_liquido_sum'] = ('valor_liquido', 'sum')
        if "Quantidade de Atendimentos" in metricas:
            agg_dict['qtd_atendimentos'] = ('valor_documento', 'count')
        if "Clientes Únicos" in metricas:
            agg_dict['clientes_unicos'] = ('cliente', 'nunique')
        if "Maior Venda" in metricas:
            agg_dict['maior_venda'] = ('valor_documento', 'max')
        if "Menor Venda" in metricas:
            agg_dict['menor_venda'] = ('valor_documento', 'min')
        
        df_custom = df.groupby(colunas_agrupamento).agg(**agg_dict).reset_index()
        
        if "Ticket Médio" in metricas:
            if 'valor_documento_sum' in df_custom.columns and 'qtd_atendimentos' in df_custom.columns:
                df_custom['ticket_medio'] = df_custom['valor_documento_sum'] / df_custom['qtd_atendimentos']
            else:
                df_temp = df.groupby(colunas_agrupamento).agg(
                    receita=('valor_documento', 'sum'),
                    qtd=('valor_documento', 'count')
                ).reset_index()
                df_temp['ticket_medio'] = df_temp['receita'] / df_temp['qtd']
                df_custom = df_custom.merge(
                    df_temp[colunas_agrupamento + ['ticket_medio']], 
                    on=colunas_agrupamento, 
                    how='left'
                )
        
        rename_map = {
            'valor_documento_sum': 'Receita Total',
            'valor_liquido_sum': 'Receita Líquida',
            'qtd_atendimentos': 'Qtd Atendimentos',
            'clientes_unicos': 'Clientes Únicos',
            'ticket_medio': 'Ticket Médio',
            'maior_venda': 'Maior Venda',
            'menor_venda': 'Menor Venda',
            'data_emissao': 'Data',
            'unidade': 'Unidade',
            'forma_pagto': 'Forma Pagamento',
            'servico': 'Serviço',
            'vendedor': 'Vendedor'
        }
        
        df_custom = df_custom.rename(columns=rename_map)
        
        colunas_numericas = [col for col in df_custom.columns if col not in dimensoes]
        if colunas_numericas:
            df_custom = df_custom.sort_values(colunas_numericas[0], ascending=False)
        
        c1, c2 = st.columns([2, 1])
        with c1:
            limite = st.slider("Limitar resultados:", 5, 100, 20, 5)
        with c2:
            mostrar_totais = st.checkbox("Mostrar totais", value=True)
        
        df_display = df_custom.head(limite).copy()
        
        if mostrar_totais and len(df_display) > 0:
            totais = {}
            for col in df_display.columns:
                if col in ['Data', 'Unidade', 'Forma Pagamento', 'Serviço', 'Vendedor']:
                    totais[col] = 'TOTAL'
                elif df_display[col].dtype in ['int64', 'float64']:
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
                    elif col in ['Maior Venda', 'Menor Venda']:
                        totais[col] = (
                            df_display[col].max() if 'Maior' in col else df_display[col].min()
                        )
                    else:
                        totais[col] = df_display[col].sum()
            
            df_display = pd.concat([df_display, pd.DataFrame([totais])], ignore_index=True)
        
        format_dict = {}
        for col in df_display.columns:
            if df_display[col].dtype in ['int64', 'float64']:
                if col in ['Receita Total', 'Receita Líquida', 'Ticket Médio', 'Maior Venda', 'Menor Venda']:
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
            "Download CSV Completo",
            csv,
            f"buddha_selfservice_{data_inicio}_{data_fim}.csv",
            "text/csv"
        )

# ---------------------- TAB 5: VENDAS ECOMMERCE -----------------
with tab_ecom:
    st.subheader("Vendas Ecommerce (Vouchers)")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

    with st.spinner("Carregando dados de ecommerce..."):
        df_ecom = load_ecommerce_data(data_inicio, data_fim)

    if df_ecom.empty:
        st.warning("Sem dados de ecommerce para o período selecionado.")
    else:
        # Garantir que colunas numéricas estejam como número
        df_ecom['COUPONS'] = pd.to_numeric(df_ecom['COUPONS'], errors='coerce')
        df_ecom['PRICE_GROSS'] = pd.to_numeric(df_ecom['PRICE_GROSS'], errors='coerce')
        df_ecom['PRICE_NET'] = pd.to_numeric(df_ecom['PRICE_NET'], errors='coerce')

        # Normalizar alguns campos de texto
        if 'PACKAGE_NAME' in df_ecom.columns:
            df_ecom['PACKAGE_NAME'] = df_ecom['PACKAGE_NAME'].fillna(df_ecom['NAME'])
        else:
            df_ecom['PACKAGE_NAME'] = df_ecom['NAME']

        if 'AFILLIATION_NAME' not in df_ecom.columns:
            df_ecom['AFILLIATION_NAME'] = "Sem Unidade"

        # KPIs principais
        c1, c2, c3, c4 = st.columns(4)

        total_pedidos = int(df_ecom['ID'].nunique())
        total_vouchers = int(len(df_ecom))  # cada linha é um voucher
        receita_bruta = df_ecom['PRICE_GROSS'].fillna(0).sum()
        receita_liquida = df_ecom['PRICE_NET'].fillna(0).sum()

        c1.metric("Pedidos Ecommerce", f"{total_pedidos:,.0f}")
        c2.metric("Vouchers Vendidos", f"{total_vouchers:,.0f}")
        c3.metric("Receita Bruta", f"R$ {receita_bruta:,.2f}")
        c4.metric("Receita Líquida", f"R$ {receita_liquida:,.2f}")

        st.markdown("---")

        # 1) Top Serviços / Pacotes
        st.markdown("### Top 10 Serviços / Pacotes Vendidos")

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

        # 2) Receita por Unidade (afiliação)
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

        st.markdown("---")

        # 3) Top Clientes em quantidade de vouchers
        st.markdown("### Top 20 Clientes por Quantidade de Vouchers")

        if 'Customer_FullName' in df_ecom.columns:
            df_cli = (
                df_ecom
                .groupby(['Customer_FullName', 'Customer_Email'])
                .agg(
                    qtde_vouchers=('ID', 'count'),
                    receita_liquida=('PRICE_NET', 'sum')
                )
                .reset_index()
                .sort_values('qtde_vouchers', ascending=False)
                .head(20)
            )

            st.dataframe(
                df_cli.rename(columns={
                    'Customer_FullName': 'Cliente',
                    'Customer_Email': 'Email',
                    'qtde_vouchers': 'Qtd Vouchers',
                    'receita_liquida': 'Receita Líquida'
                }).style.format({
                    'Qtd Vouchers': '{:,.0f}',
                    'Receita Líquida': 'R$ {:,.2f}'
                }),
                use_container_width=True,
                height=450
            )
        else:
            st.info("Campo de clientes não disponível nos dados para esse período.")

        st.markdown("---")

        # Detalhe bruto dos vouchers
        st.markdown("### Detalhe dos Vouchers (últimas 200 linhas)")
        df_ecom_view = df_ecom.sort_values('CREATED_DATE_BRAZIL', ascending=False).head(200)
        st.dataframe(df_ecom_view, use_container_width=True, height=400)

        csv_ecom = df_ecom.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV Ecommerce Completo",
            csv_ecom,
            f"buddha_ecommerce_{data_inicio}_{data_fim}.csv",
            "text/csv"
        )

# -----------------------------------------------------------------------------
# TABELA DETALHADA MOVIMENTAÇÃO
# -----------------------------------------------------------------------------
st.divider()
st.subheader("Dados Detalhados (Top 100 – Movimentação)")
st.dataframe(
    df[['data_emissao', 'unidade', 'cliente', 'servico', 'valor_documento']].head(100),
    use_container_width=True,
    height=300
)

csv = df.to_csv(index=False).encode('utf-8')
st.download_button("Download CSV Movimentação", csv, f"buddha_{data_inicio}_{data_fim}.csv", "text/csv")

st.caption("Buddha Spa Dashboard")
