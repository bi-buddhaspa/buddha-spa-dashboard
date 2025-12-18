import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Buddha Spa Analytics",
    page_icon="🧘",
    layout="wide"
)

st.markdown("""
    <style>
    /* Fundo da página */
    .stApp {
        background-color: #F5F0E6;
    }
    
    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #FFFFFF;
    }
    
    /* Cards de métricas */
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
    
    /* Títulos */
    h1 {
        color: #8B0000 !important;
        font-weight: 700 !important;
    }
    h2, h3 {
        color: #8B0000 !important;
    }
    
    /* Tabs */
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
    
    /* Botões */
    .stButton > button {
        background-color: #8B0000;
        color: #FFFFFF;
        border-radius: 5px;
        border: none;
        font-weight: 600;
    }
    .stButton > button:hover {
        background-color: #A52A2A;
    }
    
    /* Download button */
    .stDownloadButton > button {
        background-color: #8B0000;
        color: #FFFFFF;
        border-radius: 5px;
        border: none;
        font-weight: 600;
    }
    .stDownloadButton > button:hover {
        background-color: #A52A2A;
    }
    
    /* Dataframes */
    .stDataFrame {
        background-color: #FFFFFF;
        border-radius: 10px;
        padding: 10px;
    }
    
    /* Divider */
    hr {
        border-color: #8B0000;
    }
    
    /* Multiselect */
    .stMultiSelect [data-baseweb="tag"] {
        background-color: #8B0000;
    }
    
    /* Slider */
    .stSlider [data-baseweb="slider"] [role="slider"] {
        background-color: #8B0000;
    }
    
    /* Checkbox */
    .stCheckbox [data-baseweb="checkbox"] {
        border-color: #8B0000;
    }
    .stCheckbox [data-baseweb="checkbox"]:checked {
        background-color: #8B0000;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_bigquery_client():
    from google.oauth2 import service_account
    
    # Usar credenciais do Streamlit Cloud Secrets
    if 'gcp_service_account' in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"]
        )
        return bigquery.Client(credentials=credentials, project='buddha-bigdata')
    else:
        # Para ambiente local
        return bigquery.Client(project='buddha-bigdata')

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

# SIDEBAR
st.sidebar.title("Filtros")
st.sidebar.subheader("Periodo")

col1, col2 = st.sidebar.columns(2)
data_inicio = col1.date_input("De:", value=datetime(2025, 9, 1))
data_fim = col2.date_input("Ate:", value=datetime(2025, 9, 30))

try:
    unidades_disponiveis = load_unidades()
    unidades_selecionadas = st.sidebar.multiselect(
        "Unidades:",
        options=unidades_disponiveis,
        default=None
    )
except Exception as e:
    st.error(f"Erro: {e}")
    st.stop()

# CARREGAR DADOS
with st.spinner("Carregando..."):
    try:
        df = load_data(data_inicio, data_fim)
        
        if unidades_selecionadas:
            df = df[df['unidade'].str.lower().isin(unidades_selecionadas)]
            
    except Exception as e:
        st.error(f"Erro: {e}")
        st.stop()

if df.empty:
    st.warning("Sem dados")
    st.stop()

# HEADER
st.title("Buddha Spa - Dashboard")
st.caption(f"Periodo: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

# KPIS
col1, col2, col3, col4 = st.columns(4)

receita_total = df['valor_documento'].sum()
qtd_vendas = int(len(df))
ticket_medio = receita_total / qtd_vendas if qtd_vendas > 0 else 0
qtd_clientes = int(df['cliente'].nunique())

col1.metric("Receita Total", f"R$ {receita_total:,.2f}")
col2.metric("Quantidade de Vendas", f"{qtd_vendas:d}")
col3.metric("Ticket Medio", f"R$ {ticket_medio:,.2f}")
col4.metric("Clientes Unicos", f"{qtd_clientes:d}")

st.divider()

# GRAFICOS - Aplicar paleta de cores nos gráficos
color_palette = ['#8B0000', '#A52A2A', '#B22222', '#CD5C5C', '#F08080']

tab1, tab2, tab3, tab_selfservice = st.tabs(["Evolucao", "Unidades", "Top Servicos", "Self-Service"])

with tab1:
    st.subheader("Receita Diaria")
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

with tab3:
    st.subheader("Top 10 Servicos")
    df_servicos = df.groupby('servico')['valor_documento'].sum().reset_index()
    df_servicos = df_servicos.sort_values('valor_documento', ascending=False).head(10)
    fig = px.bar(df_servicos, x='valor_documento', y='servico', orientation='h', color='valor_documento',
                 color_continuous_scale=['#F08080', '#CD5C5C', '#B22222', '#A52A2A', '#8B0000'])
    fig.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6'
    )
    st.plotly_chart(fig, use_container_width=True)

with tab_selfservice:
    st.subheader("Monte Sua Propria Analise")
    st.caption("Escolha as dimensoes e metricas - tipo QlikView!")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Agrupar Por (Linhas)")
        dimensoes = st.multiselect(
            "Selecione uma ou mais dimensoes:",
            ["Data", "Unidade", "Forma de Pagamento", "Servico", "Vendedor"],
            default=["Unidade"],
            help="As linhas da sua tabela"
        )
    
    with col2:
        st.markdown("### Metricas (Colunas)")
        metricas = st.multiselect(
            "Selecione as metricas que deseja ver:",
            [
                "Receita Total",
                "Receita Liquida", 
                "Quantidade de Atendimentos",
                "Ticket Medio",
                "Clientes Unicos",
                "Maior Venda",
                "Menor Venda"
            ],
            default=["Receita Total", "Quantidade de Atendimentos", "Ticket Medio"],
            help="As colunas com os valores"
        )
    
    if not dimensoes:
        st.warning("Selecione pelo menos uma dimensao para agrupar")
    elif not metricas:
        st.warning("Selecione pelo menos uma metrica para exibir")
    else:
        # Mapear selecoes
        dim_map = {
            "Data": "data_emissao",
            "Unidade": "unidade",
            "Forma de Pagamento": "forma_pagto",
            "Servico": "servico",
            "Vendedor": "vendedor"
        }
        
        colunas_agrupamento = [dim_map[d] for d in dimensoes]
        
        # Criar agregacoes
        agg_dict = {}
        
        if "Receita Total" in metricas:
            agg_dict['valor_documento_sum'] = ('valor_documento', 'sum')
        
        if "Receita Liquida" in metricas:
            agg_dict['valor_liquido_sum'] = ('valor_liquido', 'sum')
        
        if "Quantidade de Atendimentos" in metricas:
            agg_dict['qtd_atendimentos'] = ('valor_documento', 'count')
        
        if "Clientes Unicos" in metricas:
            agg_dict['clientes_unicos'] = ('cliente', 'nunique')
        
        if "Maior Venda" in metricas:
            agg_dict['maior_venda'] = ('valor_documento', 'max')
        
        if "Menor Venda" in metricas:
            agg_dict['menor_venda'] = ('valor_documento', 'min')
        
        # Criar tabela
        df_custom = df.groupby(colunas_agrupamento).agg(**agg_dict).reset_index()
        
        # Ticket Medio
        if "Ticket Medio" in metricas:
            if 'valor_documento_sum' in df_custom.columns and 'qtd_atendimentos' in df_custom.columns:
                df_custom['ticket_medio'] = df_custom['valor_documento_sum'] / df_custom['qtd_atendimentos']
            else:
                df_temp = df.groupby(colunas_agrupamento).agg(
                    receita=('valor_documento', 'sum'),
                    qtd=('valor_documento', 'count')
                ).reset_index()
                df_temp['ticket_medio'] = df_temp['receita'] / df_temp['qtd']
                df_custom = df_custom.merge(df_temp[colunas_agrupamento + ['ticket_medio']], 
                                           on=colunas_agrupamento, how='left')
        
        # Renomear
        rename_map = {
            'valor_documento_sum': 'Receita Total',
            'valor_liquido_sum': 'Receita Liquida',
            'qtd_atendimentos': 'Qtd Atendimentos',
            'clientes_unicos': 'Clientes Unicos',
            'ticket_medio': 'Ticket Medio',
            'maior_venda': 'Maior Venda',
            'menor_venda': 'Menor Venda',
            'data_emissao': 'Data',
            'unidade': 'Unidade',
            'forma_pagto': 'Forma Pagamento',
            'servico': 'Servico',
            'vendedor': 'Vendedor'
        }
        
        df_custom = df_custom.rename(columns=rename_map)
        
        # Ordenar
        colunas_numericas = [col for col in df_custom.columns if col not in dimensoes]
        if colunas_numericas:
            df_custom = df_custom.sort_values(colunas_numericas[0], ascending=False)
        
        # Opcoes
        col1, col2 = st.columns([2, 1])
        
        with col1:
            limite = st.slider("Limitar resultados:", 5, 100, 20, 5)
        
        with col2:
            mostrar_totais = st.checkbox("Mostrar totais", value=True)
        
        df_display = df_custom.head(limite).copy()
        
        # Totais
        if mostrar_totais and len(df_display) > 0:
            totais = {}
            for col in df_display.columns:
                if col in ['Data', 'Unidade', 'Forma Pagamento', 'Servico', 'Vendedor']:
                    totais[col] = 'TOTAL'
                elif df_display[col].dtype in ['int64', 'float64']:
                    if col == 'Ticket Medio':
                        if 'Receita Total' in df_display.columns and 'Qtd Atendimentos' in df_display.columns:
                            totais[col] = df_display['Receita Total'].sum() / df_display['Qtd Atendimentos'].sum()
                        else:
                            totais[col] = df_display[col].mean()
                    elif col in ['Clientes Unicos', 'Qtd Atendimentos']:
                        totais[col] = df_display[col].sum()
                    elif col in ['Maior Venda', 'Menor Venda']:
                        totais[col] = df_display[col].max() if 'Maior' in col else df_display[col].min()
                    else:
                        totais[col] = df_display[col].sum()
            
            df_display = pd.concat([df_display, pd.DataFrame([totais])], ignore_index=True)
        
        # Formatar
        format_dict = {}
        for col in df_display.columns:
            if df_display[col].dtype in ['int64', 'float64']:
                if col in ['Receita Total', 'Receita Liquida', 'Ticket Medio', 'Maior Venda', 'Menor Venda']:
                    format_dict[col] = 'R$ {:,.2f}'
                else:
                    format_dict[col] = '{:,.0f}'
        
        # Exibir
        st.markdown("---")
        st.markdown(f"### Resultado ({len(df_custom)} linhas no total)")
        
        st.dataframe(
            df_display.style.format(format_dict),
            use_container_width=True,
            height=400
        )
        
        # Download
        csv = df_custom.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download CSV Completo",
            csv,
            f"buddha_selfservice_{data_inicio}_{data_fim}.csv",
            "text/csv"
        )

st.divider()

# TABELA PADRAO
st.subheader("Dados Detalhados (Top 100)")
st.dataframe(
    df[['data_emissao', 'unidade', 'cliente', 'servico', 'valor_documento']].head(100),
    use_container_width=True,
    height=300
)

csv = df.to_csv(index=False).encode('utf-8')
st.download_button("Download CSV", csv, f"buddha_{data_inicio}_{data_fim}.csv", "text/csv")

st.caption("Buddha Spa Dashboard")
