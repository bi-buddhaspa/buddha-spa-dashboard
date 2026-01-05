import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime
import locale

# Tentar configurar locale brasileiro para formatação
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR')
    except:
        pass

# -----------------------------------------------------------------------------
# MAPEAMENTO DE UNIDADES - BELLE ID
# (mantive seu dicionário original)
# -----------------------------------------------------------------------------
UNIDADE_BELLE_MAP = {
    'buddha spa - higienópolis': 708,
    'buddha spa - jardins': 751,
    'buddha spa - brooklin': 706,
    'buddha spa - ibirapuera': 207,
    'buddha spa - anália franco': 206,
    'buddha spa - shopping piracicaba': 210,
    'buddha spa - seridó': 758,
    'buddha spa - reebok cidade jardim': 754,
    'buddha spa - reebok vila olímpia': 756,
    'buddha spa - morumbi': 563365,
    'buddha spa - villa lobos': 280739,
    'buddha spa - alphaville flex': 1027,
    'buddha spa - pestana curitiba': 284130,
    'buddha spa - vitória': 284132,
    'buddha spa - mooca plaza shopping': 299557,
    'buddha spa - moema índios': 344370,
    'buddha spa - gran estanplaza berrini': 299559,
    'buddha spa - perdizes': 753,
    'buddha spa - quiosque pátio paulista': 411631,
    'buddha spa - indaiatuba': 432113,
    'buddha spa - club athletico paulistano': 433780,
    'buddha spa - vila leopoldina': 439074,
    'buddha spa - vogue square - rj': 436898,
    'buddha spa - são luís': 449096,
    'buddha spa - granja viana': 462897,
    'buddha spa - sorocaba': 470368,
    'buddha spa - clube hebraica': 465469,
    'buddha spa - blue tree faria lima': 480436,
    'buddha spa - são josé dos campos': 463324,
    'buddha spa - são caetano do sul': 482202,
    'buddha spa - santos aparecida': 482575,
    'buddha spa - ribeirão preto jardim botânico': 495713,
    'buddha spa - ipanema': 761,
    'buddha spa - barra shopping': 514956,
    'buddha spa - chácara klabin': 491304,
    'buddha spa - jardim pamplona shopping': 480636,
    'buddha spa - uberlândia shopping': 505654,
    'buddha spa - guestier': 527972,
    'buddha spa - chácara santo antônio': 547841,
    'buddha spa - vila são francisco': 552633,
    'buddha spa - curitiba batel': 554624,
    'buddha spa - shopping ibirapuera': 571958,
    'buddha spa - mogi das cruzes': 589126,
    'buddha spa - shopping anália franco': 591248,
    'buddha spa - blue tree thermas lins': 566497,
    'buddha spa - jardim marajoara': 591157,
    'buddha spa - moema pássaros': 591120,
    'buddha spa - ribeirão preto shopping santa úrsula': 591166,
    'buddha spa - ribeirão preto ribeirão shopping': 591244,
    'buddha spa - parque aclimação': 612165,
    'buddha spa - alto de santana': 615139,
    'buddha spa - botafogo praia shopping': 630887,
    'buddha spa - campinas cambuí': 622419,
    'buddha spa - bh shopping': 622474,
    'buddha spa - guarulhos bosque maia': 646089,
    'buddha spa - santos gonzaga': 627352,
    'buddha spa - rio preto redentora': 643686,
    'buddha spa - aquarius open mall': 648452,
    'buddha spa - litoral plaza': 661644,
    'buddha spa - campinas alphaville': 665798,
    'buddha spa - av morumbi - brooklin': 671311,
    'buddha spa - vila mascote': 671242,
    'buddha spa - alto da mooca': 706524,
    'buddha spa - braz leme': 706526,
    'buddha spa - ipiranga': 706528,
    'buddha spa - vinhedo': 713612,
    'buddha spa - shopping da gavea': 719958,
    'buddha spa - shopping trimais': 726764,
    'buddha spa - balneário shopping': 722151,
    'buddha spa - curitiba cabral': 722055,
    'buddha spa - piracicaba carlos botelho': 738437,
    'buddha spa - osasco bela vista': 738442,
    'buddha spa - tatuapé piqueri': 748591,
    'buddha spa - vila zelina': 749394,
    'buddha spa - portal do morumbi': 748603,
    'buddha spa - alto da boa vista': 746572,
    'buddha spa - praça panamericana': 765536,
    'buddha spa - jardim botânico - rj': 771858,
    'buddha spa - garten joinville': 722135,
    'buddha spa - the senses': 741733,
    'buddha spa - faria lima': 785999,
    'buddha spa - real parque': 767008,
    'buddha spa - hotel pullman vila olímpia': 795372,
    'buddha spa - belém': 766990,
    'buddha spa - recife': 795432,
    'buddha spa - belenzinho': 795397,
    'buddha spa - golden square': 794974,
    'buddha spa - butantã': 801471,
    'buddha spa - shopping jockey': 808781,
    'buddha spa - vila romana': 822734,
    'buddha spa - riviera de são lourenço': 837255,
    'buddha spa - tatuape gomes cardim': 857895,
    'buddha spa - planalto paulista': 862351,
    'buddha spa - teresina': 857883,
    'buddha spa - jardim paulista': 828253,
    'buddha spa - santo andré jardim bela vista': 865841,
    'buddha spa - shopping parque da cidade': 870951,
    'buddha spa - shopping jardim sul': 859641,
    'buddha spa - tamboré': 869747,
    'buddha spa - shopping vila olímpia': 870977,
    'buddha spa - laranjeiras': 828254,
    'buddha spa - shopping riomar aracaju': 874400,
    'buddha spa - consolação': 883751,
    'buddha spa - niterói icaraí': 891918,
    'buddha spa - jacarepagua': 883747,
    'buddha spa - itu': 882774,
    'buddha spa - recife espinheiro': 883744,
    'buddha spa - paraiso': 878903,
    'buddha spa - pinheiros joão moura': 916457,
    'buddha spa - vila olímpia': 759,
    'buddha spa - itaim bibi': 749,
    'buddha spa - funchal': 286078,
    'buddha spa - aclimação': 273819,
    'buddha spa - barra cittá américa': 763,
    'buddha spa - shopping rio sul': 762,
    'buddha spa - blue tree alphaville': 342385,
    'buddha spa - pátio paulista': 409747,
    'buddha spa - pestana são paulo': 265425,
    'buddha spa - santana parque shopping': 419107,
    'buddha spa - vila clementino': 427122,
    'buddha spa - jardim europa': 433779,
    'buddha spa - vila madalena': 449151,
    'buddha spa - campo belo': 452116,
    'buddha spa - alto da lapa': 483147,
    'buddha spa - panamby': 474445,
    'buddha spa - ecofit cerro corá': 507616,
    'buddha spa - alto de pinheiros': 516762,
    'buddha spa - brooklin nebraska': 526203,
    'buddha spa - mooca': 530997,
    'buddha spa - pompéia': 510948,
    'buddha spa - goiânia oeste': 591096,
    'buddha spa - vila nova conceição': 622423,
    'buddha spa - bourbon shopping': 627353,
    'buddha spa - morumbi town': 631395,
    'buddha spa - vila mariana': 639559,
    'buddha spa - jundiaí chácara urbana': 671256,
    'buddha spa - santo andré jardim': 646821,
    'buddha spa - maringá tiradentes': 706527
}

# -----------------------------------------------------------------------------
# FUNÇÕES DE FORMATAÇÃO BRASILEIRA
# -----------------------------------------------------------------------------
def formatar_moeda(valor):
    """Formata valor em moeda brasileira: R$ 1.234,56"""
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')

def formatar_numero(valor):
    """Formata número inteiro: 1.234"""
    if pd.isna(valor):
        return "0"
    return f"{int(valor):,}".replace(',', '.')

def formatar_percentual(valor):
    """Formata percentual: 12,34%"""
    if pd.isna(valor):
        return "0,00%"
    return f"{valor:.2f}%".replace('.', ',')

# -----------------------------------------------------------------------------
# CONFIGURAÇÃO DA PÁGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Buddha Spa Analytics",
    page_icon="🪷",
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
    col_logo_login, col_space = st.columns([1, 3])
    
    with col_logo_login:
        st.image("https://franquia.buddhaspa.com.br/wp-content/uploads/2022/04/perfil_BUDDHA_SPA_2.png", width=200)
    
    st.markdown("""
        <div style='text-align: center; padding: 20px;'>
            <h1 style='color: #8B0000;'>Portal de Franqueados - Buddha Spa</h1>
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
        .stMetric {
            background-color: #F5F0E6;
            padding: 15px;
            border-radius: 10px;
            border: 2px solid #8B0000;
        }
        .stMetric label {
            color: #8B0000 !important;
            font-weight: bold;
            font-size: 0.9rem;
        }
        .stMetric [data-testid="stMetricValue"] {
            color: #2C1810;
            font-size: 1.5rem !important;
            font-weight: 600;
        }
        .stMetric [data-testid="stMetricDelta"] {
            font-size: 0.8rem;
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
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
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
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
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
def load_ecommerce_data(data_inicio, data_fim, unidades_filtro=None):
    client = get_bigquery_client()
    
    # Construir filtro de unidades usando AFILLIATION_NAME
    filtro_unidade = ""
    if unidades_filtro and len(unidades_filtro) > 0:
        # Criar lista de possíveis nomes para comparação (com e sem "buddha spa -")
        unidades_nomes = []
        for u in unidades_filtro:
            # Adicionar nome sem prefixo (title case)
            nome_sem_prefixo = u.replace('buddha spa - ', '').title()
            unidades_nomes.append(nome_sem_prefixo)
            # Adicionar nome completo (title case)
            unidades_nomes.append(u.title())
        
        unidades_str = ','.join([f"'{nome}'" for nome in unidades_nomes])
        filtro_unidade = f"AND u.post_title IN ({unidades_str})"
    
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
        u.post_title AS AFILLIATION_NAME,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) 
         FROM `buddha-bigdata.raw.wp_posts` o
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_City,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END) 
         FROM `buddha-bigdata.raw.wp_posts` o
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_State
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    LEFT JOIN `buddha-bigdata.raw.wp_posts` u ON u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
    WHERE s.CREATED_DATE >= TIMESTAMP('2020-01-01 00:00:00', 'America/Sao_Paulo')
        AND s.USED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.USED_DATE <= TIMESTAMP('{data_fim} 23:59:59', 'America/Sao_Paulo')
        AND s.STATUS IN ('2','3')
        AND s.USED_DATE IS NOT NULL
        {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_nps_data(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()
    
    filtro_unidade = ""
    if unidade_filtro and unidade_filtro != 'TODAS':
        filtro_unidade = f"AND LOWER(unidade) = LOWER('{unidade_filtro}')"
    
    query = f"""
    SELECT 
        DATE(data) AS data,
        unidade,
        classificacao_padronizada,
        nota,
        flag_promotor,
        flag_neutro,
        flag_detrator
    FROM `buddha-bigdata.analytics.analise_nps_analytics`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
        {filtro_unidade}
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
data_inicio = col1.date_input("De:", value=datetime(2025, 1, 1), format="DD/MM/YYYY")
data_fim = col2.date_input("Até:", value=datetime(2025, 1, 31), format="DD/MM/YYYY")

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
col_logo, col_title = st.columns([1, 5])

with col_logo:
    st.image("https://franquia.buddhaspa.com.br/wp-content/uploads/2022/04/perfil_BUDDHA_SPA_2.png", width=120)

with col_title:
    st.title("Buddha Spa - Dashboard de Unidades")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

receita_total = df[valor_col].sum()
qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0

# Calcular ticket médio apenas com atendimentos que geraram receita
df_com_receita = df[df[valor_col] > 0]
qtd_atendimentos_pagos = int(df_com_receita['id_venda'].nunique())
ticket_medio = receita_total / qtd_atendimentos_pagos if qtd_atendimentos_pagos > 0 else 0

colk1, colk2, colk3, colk4 = st.columns(4)
colk1.metric("Receita Total", formatar_moeda(receita_total))
colk2.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
colk3.metric("Clientes Únicos", formatar_numero(qtd_clientes))
colk4.metric("Ticket Médio por Atendimento", formatar_moeda(ticket_medio))

# Mostrar unidades selecionadas
if is_admin and unidades_selecionadas:
    st.markdown("---")
    st.info(f"**📍 Unidades selecionadas:** {', '.join([u.title() for u in unidades_selecionadas])}")
elif not is_admin:
    st.markdown("---")
    st.info(f"**📍 Visualizando unidade:** {unidade_usuario.title()}")

st.divider()

# -----------------------------------------------------------------------------
# TABS
# -----------------------------------------------------------------------------
tab_visao, tab_atend, tab_fin, tab_mkt, tab_selfservice, tab_gloss = st.tabs(
    ["Visão Geral", "Atendimento", "Financeiro", "Marketing & Ecommerce", "Self-Service", "Ajuda / Glossário"]
)

# ---------------------- TAB: VISÃO GERAL -------------------------
with tab_visao:
    st.subheader("Evolução da Receita")
    
    # Carregar dados de todas as unidades para calcular média da rede
    with st.spinner("Calculando média da rede..."):
        try:
            df_todas_unidades = load_atendimentos(data_inicio, data_fim, unidade_filtro=None)
        except:
            df_todas_unidades = df.copy()
    
    # Verificar se há múltiplas unidades selecionadas
    if is_admin and unidades_selecionadas and len(unidades_selecionadas) > 1:
        # Gráfico com múltiplas linhas (uma por unidade) + média da rede
        df_evolucao = (
            df.groupby([data_col, 'unidade'])[valor_col]
            .sum()
            .reset_index()
            .sort_values(data_col)
        )
        
        # Calcular média da rede por data
        df_media_rede = (
            df_todas_unidades.groupby([data_col, 'unidade'])[valor_col]
            .sum()
            .reset_index()
            .groupby(data_col)[valor_col]
            .mean()
            .reset_index()
        )
        df_media_rede['unidade'] = 'Média da Rede'
        
        # Combinar dados
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        
        fig = px.line(
            df_evolucao_completo, 
            x=data_col, 
            y=valor_col, 
            color='unidade',
            markers=True,
            labels={valor_col: 'Receita (R$)', data_col: 'Data', 'unidade': 'Unidade'}
        )
        
        # Destacar linha de média com tracejado
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
        
        fig.update_layout(
            xaxis_title="Data",
            yaxis_title="Receita (R$)",
            height=400,
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(
                tickformat='%d/%m',
                tickmode='auto',
                nticks=15,
                showgrid=True,
                gridcolor='lightgray'
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='lightgray'
            )
        )
    else:
        # Gráfico com linha única + média da rede
        df_evolucao = (
            df.groupby(data_col)[valor_col]
            .sum()
            .reset_index()
            .sort_values(data_col)
        )
        df_evolucao['unidade'] = unidade_usuario.title() if not is_admin else 'Unidade Selecionada'
        
        # Calcular média da rede por data
        df_media_rede = (
            df_todas_unidades.groupby([data_col, 'unidade'])[valor_col]
            .sum()
            .reset_index()
            .groupby(data_col)[valor_col]
            .mean()
            .reset_index()
        )
        df_media_rede['unidade'] = 'Média da Rede'
        
        # Combinar dados
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        
        fig = px.line(
            df_evolucao_completo,
            x=data_col,
            y=valor_col,
            color='unidade',
            markers=True,
            labels={valor_col: 'Receita (R$)', data_col: 'Data', 'unidade': 'Unidade'}
        )
        
        # Estilizar linhas
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
            else:
                trace.line.color = '#8B0000'
        
        fig.update_layout(
            xaxis_title="Data",
            yaxis_title="Receita (R$)",
            height=400,
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(
                tickformat='%d/%m',
                tickmode='auto',
                nticks=15,
                showgrid=True,
                gridcolor='lightgray'
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='lightgray'
            )
        )
    
    # Formatar eixo Y com padrão brasileiro
    fig.update_yaxes(tickformat=",.2f")
    st.plotly_chart(fig, use_container_width=True, key="chart_evolucao_receita")
    
    st.markdown("---")
    
    # NPS Score
    st.subheader("NPS - Net Promoter Score")
    with st.spinner("Carregando dados de NPS..."):
        try:
            if is_admin and unidades_selecionadas:
                df_nps = load_nps_data(data_inicio, data_fim, unidade_filtro=None)
                df_nps = df_nps[df_nps['unidade'].str.lower().isin(unidades_selecionadas)]
            elif is_admin:
                df_nps = load_nps_data(data_inicio, data_fim, unidade_filtro=None)
            else:
                df_nps = load_nps_data(data_inicio, data_fim, unidade_filtro=unidade_usuario)
        except Exception as e:
            st.error(f"Erro ao carregar NPS: {e}")
            df_nps = pd.DataFrame()
    
    if not df_nps.empty:
        total_respostas = len(df_nps)
        promotores = int(df_nps['flag_promotor'].sum())
        neutros = int(df_nps['flag_neutro'].sum())
        detratores = int(df_nps['flag_detrator'].sum())
        
        # Cálculo do NPS
        nps_score = ((promotores - detratores) / total_respostas * 100) if total_respostas > 0 else 0
        perc_promotores = (promotores / total_respostas * 100) if total_respostas > 0 else 0
        perc_neutros = (neutros / total_respostas * 100) if total_respostas > 0 else 0
        perc_detratores = (detratores / total_respostas * 100) if total_respostas > 0 else 0
        
        col_nps1, col_nps2, col_nps3, col_nps4 = st.columns(4)
        col_nps1.metric("NPS Score", formatar_percentual(nps_score))
        col_nps2.metric("Promotores", f"{promotores} ({formatar_percentual(perc_promotores)})")
        col_nps3.metric("Neutros", f"{neutros} ({formatar_percentual(perc_neutros)})")
        col_nps4.metric("Detratores", f"{detratores} ({formatar_percentual(perc_detratores)})")
        
        # Gráfico de pizza NPS
        df_nps_dist = pd.DataFrame({
            'Classificação': ['Promotores', 'Neutros', 'Detratores'],
            'Quantidade': [promotores, neutros, detratores]
        })
        
        fig_nps = px.pie(
            df_nps_dist,
            names='Classificação',
            values='Quantidade',
            color='Classificação',
            color_discrete_map={'Promotores': '#2E7D32', 'Neutros': '#FFA726', 'Detratores': '#D32F2F'}
        )
        fig_nps.update_traces(textposition='inside', textinfo='percent')
        fig_nps.update_layout(paper_bgcolor='#F5F0E6', height=400, showlegend=True)
        st.plotly_chart(fig_nps, use_container_width=True, key="chart_nps_pizza")
    else:
        st.info("Sem dados de NPS para o período selecionado.")
    
    st.markdown("---")
    
    st.subheader("Receita por Unidade")
    df_unidades = (
        df.groupby('unidade')[valor_col]
        .sum()
        .reset_index()
        .sort_values(valor_col, ascending=False)
    )
    df_unidades['receita_fmt_label'] = df_unidades[valor_col].apply(lambda x: formatar_moeda(x))
    
    fig_u = px.bar(
        df_unidades,
        x=valor_col,
        y='unidade',
        orientation='h',
        text='receita_fmt_label',
        labels={valor_col: 'Receita (R$)', 'unidade': 'Unidade'}
    )
    fig_u.update_yaxes(autorange='reversed')
    fig_u.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
    fig_u.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        height=450,
        yaxis={'categoryorder': 'total descending'}
    )
    fig_u.update_xaxes(tickformat=",.2f")
    st.plotly_chart(fig_u, use_container_width=True, key="chart_receita_unidade_visao")

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
        
        # -------------------------------------------------------------
        # NOVA IMPLEMENTAÇÃO: mostrar gráficos separados por UNIDADE,
        # ordenados do maior para o menor (maior no topo) e com cores
        # distintas por unidade.
        # -------------------------------------------------------------
        st.markdown("### Top Terapeutas por Receita (por Unidade)")
        st.markdown("Cada unidade abaixo mostra os terapeutas ordenados do maior para o menor (maior no topo).")
        
        # Determinar quais unidades mostrar:
        if is_admin:
            # Se o admin selecionou unidades, usamos as selecionadas; caso contrário, pegamos as top N unidades por receita
            if unidades_selecionadas:
                unidades_para_plot = [u for u in unidades_selecionadas]
            else:
                # Pegar as 8 unidades com maior receita no período (para não sobrecarregar a tela)
                unidades_por_receita = (
                    df.groupby('unidade')[valor_col]
                    .sum()
                    .reset_index()
                    .sort_values(valor_col, ascending=False)
                )
                unidades_para_plot = unidades_por_receita['unidade'].head(8).tolist()
        else:
            unidades_para_plot = [unidade_usuario]
        
        # Palette de cores (varias cores distintas)
        palette = px.colors.qualitative.Dark24  # lista com 24 cores distintas
        color_map = {}
        for i, u in enumerate(unidades_para_plot):
            color_map[u] = palette[i % len(palette)]
        
        # Loop por unidade e desenhar um gráfico por unidade
        for unidade in unidades_para_plot:
            st.markdown(f"#### {unidade.title()}")
            df_un = df_terap[df_terap['unidade'] == unidade].copy()
            if df_un.empty:
                st.info("Sem terapeutas registrados para essa unidade no período selecionado.")
                continue
            
            # ordenar por receita desc e limitar top 15
            df_un = df_un.sort_values('receita', ascending=False).head(15)
            df_un['receita_fmt_label'] = df_un['receita'].apply(lambda x: formatar_moeda(x))
            
            fig_unit = px.bar(
                df_un,
                x='receita',
                y='profissional',
                orientation='h',
                text='receita_fmt_label',
                labels={'receita': 'Receita (R$)', 'profissional': 'Terapeuta'},
                color_discrete_sequence=[color_map[unidade]]  # mesma cor para todos bars da unidade
            )
            # Garantir maior no topo
            fig_unit.update_yaxes(autorange='reversed')
            fig_unit.update_traces(textposition='inside', textfont=dict(color='white', size=11))
            fig_unit.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=420,
                yaxis={'categoryorder': 'total descending'},
                margin=dict(l=150, r=20, t=30, b=30)
            )
            fig_unit.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_unit, use_container_width=True)
        
        st.markdown("---")
        
        # Mantive a tabela de performance agregada (todas unidades)
        st.markdown("### Tabela de Performance")
        df_terap_display = df_terap.copy()
        df_terap_display['receita'] = df_terap_display['receita'].apply(formatar_moeda)
        df_terap_display['qtd_atendimentos'] = df_terap_display['qtd_atendimentos'].apply(formatar_numero)
        df_terap_display['clientes_unicos'] = df_terap_display['clientes_unicos'].apply(formatar_numero)
        df_terap_display['ticket_medio'] = df_terap_display['ticket_medio'].apply(formatar_moeda)
        
        st.dataframe(
            df_terap_display,
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
            fig_s.update_yaxes(autorange='reversed')
            fig_s.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_s.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=500,
                yaxis={'categoryorder': 'total descending'}
            )
            fig_s.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_s, use_container_width=True, key="chart_principais_servicos")
        
        with cols2:
            # Formatar tabela
            df_servicos_display = df_servicos.copy()
            df_servicos_display['receita_fmt'] = df_servicos_display['receita'].apply(formatar_moeda)
            df_servicos_display['qtd_fmt'] = df_servicos_display['qtd'].apply(formatar_numero)
            df_servicos_display['perc_receita_fmt'] = df_servicos_display['perc_receita'].apply(lambda x: formatar_percentual(x*100))
            
            st.dataframe(
                df_servicos_display[['nome_servico_simplificado', 'receita_fmt', 'qtd_fmt', 'perc_receita_fmt']].rename(columns={
                    'nome_servico_simplificado': 'Serviço',
                    'receita_fmt': 'Receita',
                    'qtd_fmt': 'Quantidade',
                    'perc_receita_fmt': '% Receita'
                }),
                use_container_width=True,
                height=500
            )
    
    st.markdown("---")
    
    # HEATMAP 1: Atendimentos por Dia da Semana vs Unidade
    st.subheader("Mapa de Atendimentos - Dia da Semana vs Unidade")
    
    # Adicionar dia da semana ao dataframe
    df_heatmap = df_detalhado.copy()
    df_heatmap['dia_semana'] = pd.to_datetime(df_heatmap[data_col]).dt.day_name()
    
    # Traduzir dias da semana para português
    dias_semana_map = {
        'Monday': 'Segunda-feira',
        'Tuesday': 'Terça-feira',
        'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira',
        'Friday': 'Sexta-feira',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    df_heatmap['dia_semana'] = df_heatmap['dia_semana'].map(dias_semana_map)
    
    # Agrupar por dia da semana e unidade
    df_heatmap_unidade = (
        df_heatmap.groupby(['dia_semana', 'unidade'])
        .agg(
            qtd_atendimentos=('id_venda', 'count'),
            receita=(valor_col, 'sum')
        )
        .reset_index()
    )
    
    # Criar matriz pivot
    df_pivot_unidade = df_heatmap_unidade.pivot(
        index='dia_semana',
        columns='unidade',
        values='qtd_atendimentos'
    ).fillna(0)
    
    # Ordenar dias da semana
    dias_ordem = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
    df_pivot_unidade = df_pivot_unidade.reindex([d for d in dias_ordem if d in df_pivot_unidade.index])
    
    # Criar heatmap
    fig_heat1 = go.Figure(data=go.Heatmap(
        z=df_pivot_unidade.values,
        x=df_pivot_unidade.columns,
        y=df_pivot_unidade.index,
        colorscale='Reds',
        text=df_pivot_unidade.values,
        texttemplate='%{text:.0f}',
        textfont={"size": 10},
        colorbar=dict(title="Atendimentos")
    ))
    
    fig_heat1.update_layout(
        xaxis_title="Unidade",
        yaxis_title="Dia da Semana",
        height=400,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6'
    )
    
    st.plotly_chart(fig_heat1, use_container_width=True, key="chart_heatmap_unidade")
    
    st.markdown("---")
    
    # HEATMAP 2: Atendimentos por Dia da Semana vs Tipo de Serviço
    st.subheader("Mapa de Atendimentos - Dia da Semana vs Tipo de Serviço")
    
    if 'nome_servico_simplificado' in df_heatmap.columns:
        # Pegar top 10 serviços
        top_servicos = (
            df_heatmap.groupby('nome_servico_simplificado')
            .size()
            .sort_values(ascending=False)
            .head(10)
            .index.tolist()
        )
        
        # Filtrar apenas top serviços
        df_heatmap_servico = df_heatmap[df_heatmap['nome_servico_simplificado'].isin(top_servicos)]
        
        # Agrupar por dia da semana e serviço
        df_heatmap_servico_agg = (
            df_heatmap_servico.groupby(['dia_semana', 'nome_servico_simplificado'])
            .size()
            .reset_index(name='qtd_atendimentos')
        )
        
        # Criar matriz pivot
        df_pivot_servico = df_heatmap_servico_agg.pivot(
            index='dia_semana',
            columns='nome_servico_simplificado',
            values='qtd_atendimentos'
        ).fillna(0)
        
        # Ordenar dias da semana
        df_pivot_servico = df_pivot_servico.reindex([d for d in dias_ordem if d in df_pivot_servico.index])
        
        # Criar heatmap
        fig_heat2 = go.Figure(data=go.Heatmap(
            z=df_pivot_servico.values,
            x=df_pivot_servico.columns,
            y=df_pivot_servico.index,
            colorscale='Blues',
            text=df_pivot_servico.values,
            texttemplate='%{text:.0f}',
            textfont={"size": 10},
            colorbar=dict(title="Atendimentos")
        ))
        
        fig_heat2.update_layout(
            xaxis_title="Tipo de Serviço",
            yaxis_title="Dia da Semana",
            height=400,
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6'
        )
        
        st.plotly_chart(fig_heat2, use_container_width=True, key="chart_heatmap_servico")

# ---------------------- TAB: FINANCEIRO -------------------------
with tab_fin:
    st.subheader("Resumo Financeiro da Unidade")
    
    colf1, colf2, colf3 = st.columns(3)
    colf1.metric("Receita Total (Atendimentos)", formatar_moeda(receita_total))
    colf2.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
    colf3.metric("Ticket Médio Unidade", formatar_moeda(ticket_medio))
    
    st.markdown("---")
    
    # Distribuição de Receita (Pizza Chart)
    st.subheader("Distribuição de Receita por Canal")
    
    with st.spinner("Carregando dados de ecommerce..."):
        try:
            # Passar unidades selecionadas para filtrar ecommerce
            unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
            df_ecom_dist = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar ecommerce: {e}")
            df_ecom_dist = pd.DataFrame()
    
    # Calcular distribuição
    receita_vendas_locais = receita_total
    receita_voucher = 0
    receita_parcerias = 0
    
    if not df_ecom_dist.empty:
        df_ecom_dist['PRICE_NET'] = pd.to_numeric(df_ecom_dist['PRICE_NET'], errors='coerce')
        receita_voucher = df_ecom_dist['PRICE_NET'].fillna(0).sum()
        
        # Identificar parcerias (se houver cupom)
        if 'COUPONS' in df_ecom_dist.columns:
            df_parcerias = df_ecom_dist[df_ecom_dist['COUPONS'].notna() & (df_ecom_dist['COUPONS'] != '')]
            receita_parcerias = df_parcerias['PRICE_NET'].fillna(0).sum()
    
    faturamento_total = receita_vendas_locais + receita_voucher
    
    # Cards de resumo
    cold1, cold2, cold3, cold4 = st.columns(4)
    cold1.metric("Vendas Locais", formatar_moeda(receita_vendas_locais))
    cold2.metric("Vouchers Utilizados", formatar_moeda(receita_voucher))
    cold3.metric("Faturamento Total", formatar_moeda(faturamento_total))
    cold4.metric("Parcerias", formatar_moeda(receita_parcerias))
    
    # Pizza chart de distribuição
    df_dist = pd.DataFrame({
        'Canal': ['Vendas Locais', 'Vouchers Utilizados', 'Parcerias'],
        'Receita': [receita_vendas_locais, receita_voucher, receita_parcerias]
    })
    df_dist = df_dist[df_dist['Receita'] > 0]
    
    fig_dist = px.pie(
        df_dist,
        names='Canal',
        values='Receita',
        labels={'Receita': 'Receita (R$)', 'Canal': 'Canal'}
    )
    fig_dist.update_layout(paper_bgcolor='#F5F0E6', height=400)
    st.plotly_chart(fig_dist, use_container_width=True, key="chart_distribuicao_receita")
    
    st.markdown("---")
    
    st.subheader("Receita por Unidade")
    df_fin_unid = (
        df.groupby('unidade')[valor_col]
        .sum()
        .reset_index()
        .rename(columns={valor_col: 'receita'})
        .sort_values('receita', ascending=False)
    )
    df_fin_unid['receita_fmt_label'] = df_fin_unid['receita'].apply(lambda x: formatar_moeda(x))
    
    fig_fu = px.bar(
        df_fin_unid,
        x='receita',
        y='unidade',
        orientation='h',
        text='receita_fmt_label',
        labels={'receita': 'Receita (R$)', 'unidade': 'Unidade'}
    )
    fig_fu.update_yaxes(autorange='reversed')
    fig_fu.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
    fig_fu.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        height=450,
        yaxis={'categoryorder': 'total descending'}
    )
    fig_fu.update_xaxes(tickformat=",.2f")
    st.plotly_chart(fig_fu, use_container_width=True, key="chart_receita_unidade_fin")
    
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
            df_serv_fin['receita_fmt_label'] = df_serv_fin['receita'].apply(lambda x: formatar_moeda(x))
            
            fig_sf = px.bar(
                df_serv_fin,
                x='receita',
                y='nome_servico_simplificado',
                orientation='h',
                text='receita_fmt_label',
                labels={'receita': 'Receita (R$)', 'nome_servico_simplificado': 'Serviço'}
            )
            fig_sf.update_yaxes(autorange='reversed')
            fig_sf.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_sf.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400,
                yaxis={'categoryorder': 'total descending'}
            )
            fig_sf.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_sf, use_container_width=True, key="chart_servicos_fin")
        
        with colf_s2:
            df_serv_fin_display = df_serv_fin.copy()
            df_serv_fin_display['receita_fmt'] = df_serv_fin_display['receita'].apply(formatar_moeda)
            df_serv_fin_display['qtd_fmt'] = df_serv_fin_display['qtd'].apply(formatar_numero)
            
            st.dataframe(
                df_serv_fin_display[['nome_servico_simplificado', 'receita_fmt', 'qtd_fmt']].rename(columns={
                    'nome_servico_simplificado': 'Serviço',
                    'receita_fmt': 'Receita',
                    'qtd_fmt': 'Quantidade'
                }),
                use_container_width=True,
                height=400
            )
    
    st.markdown("---")
    
    st.subheader("Vouchers Mais Utilizados na Unidade")
    
    if not df_ecom_dist.empty:
        if 'PACKAGE_NAME' in df_ecom_dist.columns:
            df_ecom_dist['PACKAGE_NAME'] = df_ecom_dist['PACKAGE_NAME'].fillna(df_ecom_dist['NAME'])
        else:
            df_ecom_dist['PACKAGE_NAME'] = df_ecom_dist['NAME']
        
        df_ecom_top = (
            df_ecom_dist
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
            df_ecom_top['receita_fmt_label'] = df_ecom_top['receita_liquida'].apply(lambda x: formatar_moeda(x))
            
            fig_ef = px.bar(
                df_ecom_top,
                x='receita_liquida',
                y='PACKAGE_NAME',
                orientation='h',
                text='receita_fmt_label',
                labels={'receita_liquida': 'Receita Líquida (R$)', 'PACKAGE_NAME': 'Serviço / Pacote'}
            )
            fig_ef.update_yaxes(autorange='reversed')
            fig_ef.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_ef.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400,
                yaxis={'categoryorder': 'total descending'}
            )
            fig_ef.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_ef, use_container_width=True, key="chart_vouchers_fin")
        
        with colf_e2:
            df_ecom_top_display = df_ecom_top.copy()
            df_ecom_top_display['qtde_vouchers_fmt'] = df_ecom_top_display['qtde_vouchers'].apply(formatar_numero)
            df_ecom_top_display['receita_liquida_fmt'] = df_ecom_top_display['receita_liquida'].apply(formatar_moeda)
            
            st.dataframe(
                df_ecom_top_display[['PACKAGE_NAME', 'qtde_vouchers_fmt', 'receita_liquida_fmt']].rename(columns={
                    'PACKAGE_NAME': 'Serviço / Pacote',
                    'qtde_vouchers_fmt': 'Qtd Vouchers',
                    'receita_liquida_fmt': 'Receita Líquida'
                }),
                use_container_width=True,
                height=400
            )
    else:
        st.info("Sem dados de ecommerce para o período selecionado.")

# ---------------------- TAB: MARKETING & ECOMMERCE -------------------------
with tab_mkt:
    # BLOCO 1 – ECOMMERCE
    st.subheader("Ecommerce – Vouchers Utilizados na Unidade")
    
    with st.spinner("Carregando dados de vouchers utilizados..."):
        try:
            # Passar unidades selecionadas para filtrar ecommerce
            unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
            df_ecom = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar dados de ecommerce: {e}")
            df_ecom = pd.DataFrame()
    
    if df_ecom.empty:
        st.warning("Sem dados de vouchers utilizados para o período selecionado.")
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
        
        colm1.metric("Pedidos Utilizados", formatar_numero(total_pedidos))
        colm2.metric("Vouchers Utilizados", formatar_numero(total_vouchers))
        colm3.metric("Receita Vouchers Utilizados", formatar_moeda(receita_liquida_e))
        colm4.metric("Ticket Médio por Pedido", formatar_moeda(ticket_medio_e))
        
        st.markdown("### Top 10 Serviços / Pacotes Utilizados (Vouchers)")
        
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
            df_serv['qtde_fmt_label'] = df_serv['qtde_vouchers'].apply(lambda x: formatar_numero(x))
            
            fig_serv = px.bar(
                df_serv,
                x='qtde_vouchers',
                y='PACKAGE_NAME',
                orientation='h',
                labels={'qtde_vouchers': 'Qtd Vouchers', 'PACKAGE_NAME': 'Serviço / Pacote'},
                text='qtde_fmt_label'
            )
            fig_serv.update_yaxes(autorange='reversed')
            fig_serv.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_serv.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=450,
                yaxis={'categoryorder': 'total descending'}
            )
            st.plotly_chart(fig_serv, use_container_width=True, key="chart_vouchers_mkt")
        
        with col_b:
            df_serv_display = df_serv.copy()
            df_serv_display['qtde_vouchers_fmt'] = df_serv_display['qtde_vouchers'].apply(formatar_numero)
            df_serv_display['receita_liquida_fmt'] = df_serv_display['receita_liquida'].apply(formatar_moeda)
            
            st.dataframe(
                df_serv_display[['PACKAGE_NAME', 'qtde_vouchers_fmt', 'receita_liquida_fmt']].rename(columns={
                    'PACKAGE_NAME': 'Serviço / Pacote',
                    'qtde_vouchers_fmt': 'Qtd Vouchers',
                    'receita_liquida_fmt': 'Receita Líquida'
                }),
                use_container_width=True,
                height=450
            )
        
        st.markdown("---")
        
        # Análise Geográfica
        st.subheader("Distribuição Geográfica - Vendas por Estado")
        
        if 'Customer_State' in df_ecom.columns:
            df_geo = (
                df_ecom.groupby('Customer_State')
                .agg(
                    qtde_vouchers=('ID', 'count'),
                    receita=('PRICE_NET', 'sum')
                )
                .reset_index()
                .sort_values('receita', ascending=False)
                .head(10)
            )
            
            df_geo['receita_fmt_label'] = df_geo['receita'].apply(lambda x: formatar_moeda(x))
            
            fig_geo = px.bar(
                df_geo,
                x='receita',
                y='Customer_State',
                orientation='h',
                text='receita_fmt_label',
                labels={'receita': 'Receita (R$)', 'Customer_State': 'Estado'}
            )
            fig_geo.update_yaxes(autorange='reversed')
            fig_geo.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_geo.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400,
                yaxis={'categoryorder': 'total descending'}
            )
            fig_geo.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_geo, use_container_width=True, key="chart_geo_estados")
    
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
        
        colg1.metric("Pageviews Totais (Site)", formatar_numero(total_pageviews))
        colg2.metric("Usuários Totais", formatar_numero(total_usuarios))
        colg3.metric("Duração Média da Sessão (s)", f"{duracao_media:,.1f}".replace('.', ','))
        
        st.markdown("### Pageviews por Tipo de Página")
        
        df_tipo = (
            df_ga4_pages.groupby('tipo_pagina')['page_views']
            .sum()
            .reset_index()
            .sort_values('page_views', ascending=False)
        )
        
        df_tipo['pageviews_fmt_label'] = df_tipo['page_views'].apply(lambda x: formatar_numero(int(x)))
        
        fig_pag = px.bar(
            df_tipo,
            x='page_views',
            y='tipo_pagina',
            orientation='h',
            text='pageviews_fmt_label',
            labels={'page_views': 'Pageviews', 'tipo_pagina': 'Tipo de Página'}
        )
        fig_pag.update_yaxes(autorange='reversed')
        fig_pag.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
        fig_pag.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400,
            yaxis={'categoryorder': 'total descending'}
        )
        st.plotly_chart(fig_pag, use_container_width=True, key="chart_pageviews_ga4")
    
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
        
        colt1.metric("Sessões Totais", formatar_numero(total_sessoes))
        colt2.metric("Usuários Totais", formatar_numero(total_usuarios_t))
        colt3.metric("Novos Usuários", formatar_numero(total_novos))
        
        st.markdown("### Sessões por Canal")
        
        df_canal = (
            df_ga4_traffic.groupby('canal')['sessoes']
            .sum()
            .reset_index()
            .sort_values('sessoes', ascending=False)
        )
        
        df_canal['sessoes_fmt_label'] = df_canal['sessoes'].apply(lambda x: formatar_numero(int(x)))
        
        fig_can = px.bar(
            df_canal,
            x='sessoes',
            y='canal',
            orientation='h',
            text='sessoes_fmt_label',
            labels={'canal': 'Canal', 'sessoes': 'Sessões'}
        )
        fig_can.update_yaxes(autorange='reversed')
        fig_can.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
        fig_can.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400,
            yaxis={'categoryorder': 'total descending'}
        )
        st.plotly_chart(fig_can, use_container_width=True, key="chart_sessoes_canal")
        
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
        st.plotly_chart(fig_disp, use_container_width=True, key="chart_dispositivos")
    
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
            
            df_eventos_agg['eventos_fmt_label'] = df_eventos_agg['total_eventos'].apply(lambda x: formatar_numero(int(x)))
            
            fig_ev = px.bar(
                df_eventos_agg,
                x='total_eventos',
                y='evento',
                orientation='h',
                text='eventos_fmt_label',
                labels={'total_eventos': 'Total de Eventos', 'evento': 'Evento'}
            )
            fig_ev.update_yaxes(autorange='reversed')
            fig_ev.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_ev.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400,
                yaxis={'categoryorder': 'total descending'}
            )
            st.plotly_chart(fig_ev, use_container_width=True, key="chart_eventos_ga4")
    
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
        
        coli1.metric("Total de Posts", formatar_numero(total_posts))
        coli2.metric("Total de Curtidas", formatar_numero(total_curtidas))
        coli3.metric("Total de Comentários", formatar_numero(total_coment))
        coli4.metric("Total de Impressões", formatar_numero(total_impressoes))
        
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
            df_top_eng['engajamento_fmt_label'] = df_top_eng['engajamento'].apply(lambda x: formatar_numero(int(x)))
            
            fig_ig = px.bar(
                df_top_eng,
                x='engajamento',
                y='legenda_curta',
                orientation='h',
                labels={'engajamento': 'Engajamento (Curtidas + Comentários)', 'legenda_curta': 'Post'},
                text='engajamento_fmt_label'
            )
            fig_ig.update_yaxes(autorange='reversed')
            fig_ig.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_ig.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=500,
                yaxis={'categoryorder': 'total descending'}
            )
            st.plotly_chart(fig_ig, use_container_width=True, key="chart_instagram_posts")
        
        with colg_b:
            st.markdown("#### Detalhes dos Top Posts")
            st.dataframe(
                df_top_eng[[
                    'data_post',
                    'nome',
                    'visualizacoes',
                    'curtidas',
                    'comentarios',
                    'compartilhamentos',
                    'alcance'
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
        cols1.metric("Seguidores Atuais", formatar_numero(seg_fim))
        cols2.metric("Crescimento no Período", f"{crescimento:+,}".replace(',', '.'), delta=formatar_percentual(perc_crescimento))
        cols3.metric("Seguidores no Início", formatar_numero(seg_inicio))
        
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
        st.plotly_chart(fig_seg, use_container_width=True, key="chart_seguidores_ig")
    
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
        
        m1.metric("Impressões", formatar_numero(total_imp))
        m2.metric("Cliques", formatar_numero(total_clicks), delta=f"CTR {formatar_percentual(ctr)}")
        m3.metric("Investimento (R$)", formatar_moeda(total_invest))
        m4.metric("Receita Atribuída (R$)", formatar_moeda(total_vendas_valor), delta=f"ROI {formatar_percentual(roi)}")
        
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
        df_meta_top['investido_fmt_label'] = df_meta_top['investido'].apply(lambda x: formatar_moeda(x))
        
        fig_meta = px.bar(
            df_meta_top,
            x='investido',
            y='nome',
            orientation='h',
            text='investido_fmt_label',
            labels={'investido': 'Investimento (R$)', 'nome': 'Campanha'}
        )
        fig_meta.update_yaxes(autorange='reversed')
        fig_meta.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
        fig_meta.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=450,
            yaxis={'categoryorder': 'total descending'}
        )
        fig_meta.update_xaxes(tickformat=",.2f")
        st.plotly_chart(fig_meta, use_container_width=True, key="chart_meta_ads")
        
        st.markdown("#### Detalhes das Campanhas (Top 10 por Investimento)")
        
        df_meta_top_display = df_meta_top.copy()
        df_meta_top_display['impressoes_fmt'] = df_meta_top_display['impressoes'].apply(formatar_numero)
        df_meta_top_display['cliques_fmt'] = df_meta_top_display['cliques'].apply(formatar_numero)
        df_meta_top_display['vendas_fmt'] = df_meta_top_display['vendas'].apply(formatar_numero)
        df_meta_top_display['investido_fmt'] = df_meta_top_display['investido'].apply(formatar_moeda)
        df_meta_top_display['vendas_valor_fmt'] = df_meta_top_display['vendas_valor'].apply(formatar_moeda)
        df_meta_top_display['ctr_fmt'] = df_meta_top_display['ctr'].apply(lambda x: formatar_percentual(x*100))
        df_meta_top_display['cpc_fmt'] = df_meta_top_display['cpc'].apply(formatar_moeda)
        df_meta_top_display['roi_fmt'] = df_meta_top_display['roi'].apply(lambda x: formatar_percentual(x*100))
        
        st.dataframe(
            df_meta_top_display[[
                'nome',
                'impressoes_fmt',
                'cliques_fmt',
                'vendas_fmt',
                'investido_fmt',
                'vendas_valor_fmt',
                'ctr_fmt',
                'cpc_fmt',
                'roi_fmt'
            ]].rename(columns={
                'nome': 'Campanha',
                'impressoes_fmt': 'Impressões',
                'cliques_fmt': 'Cliques',
                'vendas_fmt': 'Vendas',
                'investido_fmt': 'Investido (R$)',
                'vendas_valor_fmt': 'Receita (R$)',
                'ctr_fmt': 'CTR',
                'cpc_fmt': 'CPC',
                'roi_fmt': 'ROI'
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
        
        # Mapeamento de nomes amigáveis
        nomes_amigaveis = {
            data_col: "Data",
            "unidade": "Unidade",
            "forma_pagamento": "Forma de Pagamento",
            "nome_servico_simplificado": "Serviço",
            "profissional": "Terapeuta",
            "nome_cliente": "Cliente",
            "receita_total": "Receita Total",
            "qtd_atendimentos": "Quantidade de Atendimentos",
            "ticket_medio": "Ticket Médio",
            "clientes_unicos": "Clientes Únicos"
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
            
            # Formatar para exibição
            df_display = df_custom.copy()
            
            # Formatar datas para dd/mm/yyyy
            if data_col in df_display.columns:
                df_display[data_col] = pd.to_datetime(df_display[data_col]).dt.strftime('%d/%m/%Y')
            
            # Formatar valores monetários
            if 'receita_total' in df_display.columns:
                df_display['receita_total'] = df_display['receita_total'].apply(formatar_moeda)
            
            if 'ticket_medio' in df_display.columns:
                df_display['ticket_medio'] = df_display['ticket_medio'].apply(formatar_moeda)
            
            # Formatar números inteiros
            if 'qtd_atendimentos' in df_display.columns:
                df_display['qtd_atendimentos'] = df_display['qtd_atendimentos'].apply(formatar_numero)
            
            if 'clientes_unicos' in df_display.columns:
                df_display['clientes_unicos'] = df_display['clientes_unicos'].apply(formatar_numero)
            
            # Renomear colunas para nomes amigáveis
            df_display = df_display.rename(columns=nomes_amigaveis)
            
            st.markdown("---")
            st.dataframe(df_display, use_container_width=True, height=400)
            
            # Para download, manter valores numéricos mas formatar datas
            df_download = df_custom.copy()
            if data_col in df_download.columns:
                df_download[data_col] = pd.to_datetime(df_download[data_col]).dt.strftime('%d/%m/%Y')
            df_download = df_download.rename(columns=nomes_amigaveis)
            
            csv = df_download.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button(
                "📥 Download CSV",
                csv,
                f"buddha_selfservice_{data_inicio.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.csv",
                "text/csv",
                key='download-csv'
            )

# ---------------------- TAB: AJUDA / GLOSSÁRIO -------------------------
with tab_gloss:
    st.subheader("Ajuda / Glossário de Métricas")
    
    st.markdown("""
    ### 📊 Principais Métricas
    
    **Receita Total** – Soma de todos os valores líquidos de atendimentos no período.
    
    **Quantidade de Atendimentos** – Número de atendimentos únicos (`id_venda`).
    
    **Clientes Únicos** – Número de clientes distintos atendidos.
    
    **Ticket Médio por Atendimento** – Receita Total ÷ Quantidade de Atendimentos.
    
    **NPS (Net Promoter Score)** – Indicador de satisfação calculado como: (% Promotores - % Detratores).
    - **Promotores**: Notas 9-10
    - **Neutros**: Notas 7-8
    - **Detratores**: Notas 0-6
    
    **Serviços Presenciais Mais Vendidos** – Ranking de serviços presenciais por receita e quantidade.
    
    **Vouchers Utilizados** – Vouchers do ecommerce que foram efetivamente utilizados na unidade (filtrados por `USED_DATE` e `AFILLIATION_NAME`).
    
    **Distribuição de Receita** – Divisão da receita entre Vendas Locais, Vouchers Utilizados e Parcerias.
    
    **Pageviews (GA4)** – Visualizações de página no site / páginas-chave.
    
    **Sessões (GA4)** – Sessões por canal de aquisição (Direct, Organic, Paid, Social etc.).
    
    **Eventos (GA4)** – Eventos como `form_submit`, cliques, WhatsApp etc.
    
    **Seguidores Instagram** – Evolução de `qtd_seguidores` ao longo do tempo.
    
    **Meta Ads** – Impressões, cliques, investimento, vendas e ROI das campanhas.
    - **CTR (Click-Through Rate)**: Cliques ÷ Impressões × 100
    - **CPC (Custo Por Clique)**: Investimento ÷ Cliques
    - **ROI (Return on Investment)**: (Receita - Investimento) ÷ Investimento × 100
    
    ### 🎫 Sobre Vouchers
    
    **Importante:** Os vouchers são vendidos no ecommerce geral (site Buddha Spa) e podem ser utilizados em qualquer unidade. 
    
    Neste dashboard, você vê apenas os **vouchers que foram utilizados na sua unidade**, não os vendidos. A data considerada é a `USED_DATE` (quando o cliente usou o voucher), não a `CREATED_DATE` (quando comprou).
    """)
     
    st.caption("Buddha Spa Dashboard – Portal de Franqueados v2.0")
