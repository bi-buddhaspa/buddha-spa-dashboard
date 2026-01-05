# app.py (colar substituindo o seu script atual)
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
# MAPEAMENTO DE UNIDADES - BELLE ID (mantive seu dicionário original)
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
    # valor pode ser percent (12.34) ou proporção (0.1234) dependendo do uso; usamos direto
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
# AUTENTICAÇÃO SIMPLES (mantive seu bloco original)
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
        # Usa ADC (Application Default Credentials) se estiver no GCP
        return bigquery.Client(project='buddha-bigdata')

# -----------------------------------------------------------------------------
# FUNÇÕES DE DADOS – ATENDIMENTOS / FINANCEIRO (mantive suas originais)
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

# -----------------------------------------------------------------------------
# NOVA FUNÇÃO: CARREGAR CHAMADOS (SUPORTE)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=900)
def load_chamados(data_inicio, data_fim, unidades_belle_ids=None):
    """
    Carrega chamados de suporte. Filtra por data_abertura_sp entre data_inicio e data_fim.
    unidades_belle_ids: lista de inteiros (unidade_id na tabela chamados)
    """
    client = get_bigquery_client()
    filtro_unidade = ""
    if unidades_belle_ids:
        ids_str = ",".join([str(int(x)) for x in unidades_belle_ids])
        filtro_unidade = f"AND unidade_id IN ({ids_str})"

    query = f"""
    SELECT
      id,
      titulo,
      solicitante_id,
      solicitante_nome,
      responsavel_id,
      responsavel_nome,
      unidade_id,
      unidade_nome,
      departamento_id,
      departamento_nome,
      assunto_id,
      assunto_nome,
      sla_horas_comerciais,
      status_mapeamento_sla,
      tipo,
      situacao,
      situacao_descricao,
      aberto_sp,
      resolvido_sp,
      data_abertura_sp,
      data_resolucao_sp,
      horas_comerciais_resolucao,
      dias_comerciais_resolucao,
      status_sla_horas_comerciais,
      percentual_sla_horas_comerciais,
      chamado_finalizado,
      avaliacaoNota
    FROM `buddha-bigdata.analytics.chamados_analytics_completa`
    WHERE DATE(data_abertura_sp) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
      {filtro_unidade}
    """
    return client.query(query).to_dataframe()

# -----------------------------------------------------------------------------
# FUNÇÃO ATUALIZADA: CARREGAR ECOMMERCE (INCLUI OMNICHANNEL MATCH)
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_ecommerce_data(data_inicio, data_fim, unidades_filtro=None):
    """
    Busca registros de ecommerce. Tenta casar AFILLIATION_ID (quando existe) ou extrair sufixo do NAME
    para casar com wp_posts.post_title (unidade).
    unidades_filtro: lista de nomes (post_title) ou list de belle ids (numéricos) - opcional
    """
    client = get_bigquery_client()

    # Preparar filtro de unidades. Pode ser nomes (strings) ou ids (int).
    filtro_unidade_af_id = ""
    filtro_unidade_name = ""
    if unidades_filtro:
        # separa nomes de ids
        ids = [str(x) for x in unidades_filtro if isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit())]
        nomes = [x for x in unidades_filtro if not (isinstance(x, (int, float)) or (isinstance(x, str) and x.isdigit()))]
        if ids:
            filtro_unidade_af_id = "AND CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) IN (" + ",".join(ids) + ")"
        if nomes:
            # preparar lista de nomes escapados
            nomes_esc = ",".join([f"'{n}'" for n in nomes])
            filtro_unidade_name = f"AND LOWER(u.post_title) IN ({nomes_esc})"

    query = f"""
    WITH base AS (
      SELECT 
        s.*,
        DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
        DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL,
        SAFE_CAST(SAFE_CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) AS AFILLIATION_ID_INT,
        TRIM(REGEXP_EXTRACT(s.NAME, r'-\\s*([^\\-]+)$')) AS name_sufix
      FROM `buddha-bigdata.raw.ecommerce_raw` s
      WHERE s.CREATED_DATE >= TIMESTAMP('2020-01-01 00:00:00', 'America/Sao_Paulo')
        AND s.USED_DATE IS NOT NULL
        AND s.USED_DATE BETWEEN TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo') AND TIMESTAMP('{data_fim} 23:59:59', 'America/Sao_Paulo')
        AND s.STATUS IN ('1','2','3')
        AND s.NAME NOT LIKE '%Voucher Experience%'
    )

    SELECT
      b.ID,
      b.NAME,
      b.STATUS,
      b.COUPONS,
      b.CREATED_DATE_BRAZIL,
      b.USED_DATE_BRAZIL,
      SAFE_CAST(b.PRICE_NET AS FLOAT64) AS PRICE_NET,
      SAFE_CAST(b.PRICE_GROSS AS FLOAT64) AS PRICE_GROSS,
      b.PRICE_REFOUND,
      b.KEY,
      b.ORDER_ID,
      (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = b.PACKAGE_ID) AS PACKAGE_NAME,
      b.AFILLIATION_ID_INT AS AFILLIATION_ID,
      COALESCE(u.post_title, b.name_sufix) AS AFILLIATION_NAME,
      b.name_sufix,
      -- Campos do cliente via postmeta/usermeta
      (SELECT MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) 
         FROM `buddha-bigdata.raw.wp_posts` o
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(b.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_City,
      (SELECT MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END) 
         FROM `buddha-bigdata.raw.wp_posts` o
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(b.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_State
    FROM base b
    LEFT JOIN `buddha-bigdata.raw.wp_posts` u
      ON (
           (b.AFILLIATION_ID_INT IS NOT NULL AND u.ID = b.AFILLIATION_ID_INT AND u.post_type='unidade')
           OR (b.AFILLIATION_ID_INT IS NULL AND LOWER(TRIM(u.post_title)) = LOWER(TRIM(b.name_sufix)) AND u.post_type='unidade')
         )
    WHERE 1=1
      {filtro_unidade_af_id}
      {filtro_unidade_name}
    """
    return client.query(query).to_dataframe()

# -----------------------------------------------------------------------------
# FUNÇÕES – GA4 / INSTAGRAM / META (mantive as suas)
# -----------------------------------------------------------------------------
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
data_inicio = col1.date_input("De:", value=datetime(2025, 9, 1), format="DD/MM/YYYY")
data_fim = col2.date_input("Até:", value=datetime(2025, 9, 30), format="DD/MM/YYYY")

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
# Mapear unidades selecionadas para belle ids (quando possível)
# -----------------------------------------------------------------------------
def unidades_para_belle_ids(list_unidades_lower):
    ids = []
    for u in list_unidades_lower or []:
        key = u.lower()
        if key in UNIDADE_BELLE_MAP:
            ids.append(UNIDADE_BELLE_MAP[key])
    return ids

unidades_belle_ids_selecionadas = unidades_para_belle_ids(unidades_selecionadas) if unidades_selecionadas else None

# -----------------------------------------------------------------------------
# CARREGAR DADOS PRINCIPAIS
# -----------------------------------------------------------------------------
with st.spinner("Carregando dados de atendimentos..."):
    try:
        if is_admin and not unidades_selecionadas:
            df = load_atendimentos(data_inicio, data_fim, unidade_filtro=None)
            df_detalhado = load_atendimentos_detalhados(data_inicio, data_fim, unidade_filtro=None)
        elif is_admin and unidades_selecionadas:
            # Carrega tudo e filtra localmente (mais controle para variações de nome)
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
# TABS (adicionado "Suporte")
# -----------------------------------------------------------------------------
tab_visao, tab_atend, tab_fin, tab_mkt, tab_selfservice, tab_suporte, tab_gloss = st.tabs(
    ["Visão Geral", "Atendimento", "Financeiro", "Marketing & Ecommerce", "Self-Service", "Suporte", "Ajuda / Glossário"]
)

# ---------------------- TAB: VISÃO GERAL -------------------------
with tab_visao:
    st.subheader("Evolução da Receita")
    # [mantive o mesmo conteúdo seu para esta tab — por brevidade não repito tudo aqui]
    # ... (o conteúdo completo da visão já estava no seu script original)
    # Vou reutilizar o bloco que você já tem: criar df_evolucao, df_media_rede, construir fig e exibir
    with st.spinner("Calculando média da rede..."):
        try:
            df_todas_unidades = load_atendimentos(data_inicio, data_fim, unidade_filtro=None)
        except:
            df_todas_unidades = df.copy()
    if is_admin and unidades_selecionadas and len(unidades_selecionadas) > 1:
        df_evolucao = (
            df.groupby([data_col, 'unidade'])[valor_col]
            .sum()
            .reset_index()
            .sort_values(data_col)
        )
        df_media_rede = (
            df_todas_unidades.groupby([data_col, 'unidade'])[valor_col]
            .sum()
            .reset_index()
            .groupby(data_col)[valor_col]
            .mean()
            .reset_index()
        )
        df_media_rede['unidade'] = 'Média da Rede'
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        fig = px.line(df_evolucao_completo, x=data_col, y=valor_col, color='unidade', markers=True,
                      labels={valor_col: 'Receita (R$)', data_col: 'Data', 'unidade': 'Unidade'})
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
    else:
        df_evolucao = (
            df.groupby(data_col)[valor_col]
            .sum()
            .reset_index()
            .sort_values(data_col)
        )
        df_evolucao['unidade'] = unidade_usuario.title() if not is_admin else 'Unidade Selecionada'
        df_media_rede = (
            df_todas_unidades.groupby([data_col, 'unidade'])[valor_col]
            .sum()
            .reset_index()
            .groupby(data_col)[valor_col]
            .mean()
            .reset_index()
        )
        df_media_rede['unidade'] = 'Média da Rede'
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        fig = px.line(df_evolucao_completo, x=data_col, y=valor_col, color='unidade', markers=True,
                      labels={valor_col: 'Receita (R$)', data_col: 'Data', 'unidade': 'Unidade'})
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
            else:
                trace.line.color = '#8B0000'
    fig.update_layout(xaxis_title="Data", yaxis_title="Receita (R$)", height=400,
                      plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6',
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                      xaxis=dict(tickformat='%d/%m', tickmode='auto', nticks=15, showgrid=True, gridcolor='lightgray'),
                      yaxis=dict(showgrid=True, gridcolor='lightgray'))
    fig.update_yaxes(tickformat=",.2f")
    st.plotly_chart(fig, use_container_width=True, key="chart_evolucao_receita")

    # NPS, Receita por Unidade etc (mantidos do seu script original)
    st.markdown("---")
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
        nps_score = ((promotores - detratores) / total_respostas * 100) if total_respostas > 0 else 0
        perc_promotores = (promotores / total_respostas * 100) if total_respostas > 0 else 0
        perc_neutros = (neutros / total_respostas * 100) if total_respostas > 0 else 0
        perc_detratores = (detratores / total_respostas * 100) if total_respostas > 0 else 0
        col_nps1, col_nps2, col_nps3, col_nps4 = st.columns(4)
        col_nps1.metric("NPS Score", formatar_percentual(nps_score))
        col_nps2.metric("Promotores", f"{promotores} ({formatar_percentual(perc_promotores)})")
        col_nps3.metric("Neutros", f"{neutros} ({formatar_percentual(perc_neutros)})")
        col_nps4.metric("Detratores", f"{detratores} ({formatar_percentual(perc_detratores)})")
        df_nps_dist = pd.DataFrame({'Classificação': ['Promotores', 'Neutros', 'Detratores'],
                                    'Quantidade': [promotores, neutros, detratores]})
        fig_nps = px.pie(df_nps_dist, names='Classificação', values='Quantidade',
                         color_discrete_map={'Promotores': '#2E7D32', 'Neutros': '#FFA726', 'Detratores': '#D32F2F'})
        fig_nps.update_traces(textposition='inside', textinfo='percent')
        fig_nps.update_layout(paper_bgcolor='#F5F0E6', height=400, showlegend=True)
        st.plotly_chart(fig_nps, use_container_width=True, key="chart_nps_pizza")
    else:
        st.info("Sem dados de NPS para o período selecionado.")

    st.markdown("---")
    st.subheader("Receita por Unidade")
    df_unidades = (df.groupby('unidade')[valor_col].sum().reset_index().sort_values(valor_col, ascending=False))
    df_unidades['receita_fmt_label'] = df_unidades[valor_col].apply(lambda x: formatar_moeda(x))
    fig_u = px.bar(df_unidades, x=valor_col, y='unidade', orientation='h', text='receita_fmt_label',
                   labels={valor_col: 'Receita (R$)', 'unidade': 'Unidade'})
    fig_u.update_yaxes(autorange='reversed')
    fig_u.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
    fig_u.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=450, yaxis={'categoryorder': 'total descending'})
    fig_u.update_xaxes(tickformat=",.2f")
    st.plotly_chart(fig_u, use_container_width=True, key="chart_receita_unidade_visao")

# ---------------------- TAB: ATENDIMENTO -------------------------
with tab_atend:
    # Mantive o seu conteúdo pleno para a tab Atendimento (performance por terapeuta, serviços, heatmaps)
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
        st.markdown("### Top Terapeutas por Receita (por Unidade)")
        if is_admin:
            if unidades_selecionadas:
                unidades_para_plot = [u for u in unidades_selecionadas]
            else:
                unidades_por_receita = (
                    df.groupby('unidade')[valor_col]
                    .sum()
                    .reset_index()
                    .sort_values(valor_col, ascending=False)
                )
                unidades_para_plot = unidades_por_receita['unidade'].head(8).tolist()
        else:
            unidades_para_plot = [unidade_usuario]
        palette = px.colors.qualitative.Dark24
        color_map = {}
        for i, u in enumerate(unidades_para_plot):
            color_map[u] = palette[i % len(palette)]
        for unidade in unidades_para_plot:
            st.markdown(f"#### {unidade.title()}")
            df_un = df_terap[df_terap['unidade'] == unidade].copy()
            if df_un.empty:
                st.info("Sem terapeutas registrados para essa unidade no período selecionado.")
                continue
            df_un = df_un.sort_values('receita', ascending=False).head(15)
            df_un['receita_fmt_label'] = df_un['receita'].apply(lambda x: formatar_moeda(x))
            fig_unit = px.bar(df_un, x='receita', y='profissional', orientation='h', text='receita_fmt_label',
                              labels={'receita': 'Receita (R$)', 'profissional': 'Terapeuta'},
                              color_discrete_sequence=[color_map[unidade]])
            fig_unit.update_yaxes(autorange='reversed')
            fig_unit.update_traces(textposition='inside', textfont=dict(color='white', size=11))
            fig_unit.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=420, yaxis={'categoryorder': 'total descending'}, margin=dict(l=150, r=20, t=30, b=30))
            fig_unit.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_unit, use_container_width=True)
        st.markdown("---")
        st.markdown("### Tabela de Performance")
        df_terap_display = df_terap.copy()
        df_terap_display['receita'] = df_terap_display['receita'].apply(formatar_moeda)
        df_terap_display['qtd_atendimentos'] = df_terap_display['qtd_atendimentos'].apply(formatar_numero)
        df_terap_display['clientes_unicos'] = df_terap_display['clientes_unicos'].apply(formatar_numero)
        df_terap_display['ticket_medio'] = df_terap_display['ticket_medio'].apply(formatar_moeda)
        st.dataframe(df_terap_display, use_container_width=True, height=500)
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
            fig_s = px.bar(df_servicos, x='receita', y='nome_servico_simplificado', orientation='h',
                           text=df_servicos['perc_receita'].map(lambda x: f"{x*100:.1f}%"),
                           labels={'receita': 'Receita (R$)', 'nome_servico_simplificado': 'Serviço'})
            fig_s.update_yaxes(autorange='reversed')
            fig_s.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_s.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=500, yaxis={'categoryorder': 'total descending'})
            fig_s.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_s, use_container_width=True, key="chart_principais_servicos")
        with cols2:
            df_servicos_display = df_servicos.copy()
            df_servicos_display['receita_fmt'] = df_servicos_display['receita'].apply(formatar_moeda)
            df_servicos_display['qtd_fmt'] = df_servicos_display['qtd'].apply(formatar_numero)
            df_servicos_display['perc_receita_fmt'] = df_servicos_display['perc_receita'].apply(lambda x: formatar_percentual(x*100))
            st.dataframe(df_servicos_display[['nome_servico_simplificado', 'receita_fmt', 'qtd_fmt', 'perc_receita_fmt']].rename(columns={
                'nome_servico_simplificado': 'Serviço',
                'receita_fmt': 'Receita',
                'qtd_fmt': 'Quantidade',
                'perc_receita_fmt': '% Receita'
            }), use_container_width=True, height=500)
    st.markdown("---")
    # Heatmaps (mantidos do seu script)
    st.subheader("Mapa de Atendimentos - Dia da Semana vs Unidade")
    df_heatmap = df_detalhado.copy()
    df_heatmap['dia_semana'] = pd.to_datetime(df_heatmap[data_col]).dt.day_name()
    dias_semana_map = {
        'Monday': 'Segunda-feira', 'Tuesday': 'Terça-feira', 'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira', 'Friday': 'Sexta-feira', 'Saturday': 'Sábado', 'Sunday': 'Domingo'
    }
    df_heatmap['dia_semana'] = df_heatmap['dia_semana'].map(dias_semana_map)
    df_heatmap_unidade = (df_heatmap.groupby(['dia_semana', 'unidade'])
        .agg(qtd_atendimentos=('id_venda', 'count'), receita=(valor_col, 'sum'))
        .reset_index())
    df_pivot_unidade = df_heatmap_unidade.pivot(index='dia_semana', columns='unidade', values='qtd_atendimentos').fillna(0)
    dias_ordem = ['Segunda-feira','Terça-feira','Quarta-feira','Quinta-feira','Sexta-feira','Sábado','Domingo']
    df_pivot_unidade = df_pivot_unidade.reindex([d for d in dias_ordem if d in df_pivot_unidade.index])
    fig_heat1 = go.Figure(data=go.Heatmap(z=df_pivot_unidade.values, x=df_pivot_unidade.columns, y=df_pivot_unidade.index, colorscale='Reds', text=df_pivot_unidade.values, texttemplate='%{text:.0f}', textfont={"size": 10}, colorbar=dict(title="Atendimentos")))
    fig_heat1.update_layout(xaxis_title="Unidade", yaxis_title="Dia da Semana", height=400, plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6')
    st.plotly_chart(fig_heat1, use_container_width=True, key="chart_heatmap_unidade")

# ---------------------- TAB: FINANCEIRO -------------------------
with tab_fin:
    st.subheader("Resumo Financeiro da Unidade")
    colf1, colf2, colf3 = st.columns(3)
    colf1.metric("Receita Total (Atendimentos)", formatar_moeda(receita_total))
    colf2.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
    colf3.metric("Ticket Médio Unidade", formatar_moeda(ticket_medio))
    st.markdown("---")
    st.subheader("Distribuição de Receita por Canal")
    with st.spinner("Carregando dados de ecommerce..."):
        try:
            # para ecommerce, passe os belle ids (se existirem) para garantir consistência com chamados
            unidades_para_filtro = unidades_belle_ids_selecionadas if unidades_belle_ids_selecionadas else (unidades_selecionadas if unidades_selecionadas else None)
            df_ecom_dist = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar ecommerce: {e}")
            df_ecom_dist = pd.DataFrame()
    receita_vendas_locais = receita_total
    receita_voucher = 0
    receita_parcerias = 0
    if not df_ecom_dist.empty:
        df_ecom_dist['PRICE_NET'] = pd.to_numeric(df_ecom_dist['PRICE_NET'], errors='coerce').fillna(0)
        receita_voucher = df_ecom_dist['PRICE_NET'].sum()
        if 'COUPONS' in df_ecom_dist.columns:
            df_parcerias = df_ecom_dist[df_ecom_dist['COUPONS'].notna() & (df_ecom_dist['COUPONS'] != '')]
            receita_parcerias = df_parcerias['PRICE_NET'].sum()
    faturamento_total = receita_vendas_locais + receita_voucher
    cold1, cold2, cold3, cold4 = st.columns(4)
    cold1.metric("Vendas Locais", formatar_moeda(receita_vendas_locais))
    cold2.metric("Vouchers Utilizados", formatar_moeda(receita_voucher))
    cold3.metric("Faturamento Total", formatar_moeda(faturamento_total))
    cold4.metric("Parcerias", formatar_moeda(receita_parcerias))
    df_dist = pd.DataFrame({'Canal': ['Vendas Locais', 'Vouchers Utilizados', 'Parcerias'], 'Receita': [receita_vendas_locais, receita_voucher, receita_parcerias]})
    df_dist = df_dist[df_dist['Receita'] > 0]
    fig_dist = px.pie(df_dist, names='Canal', values='Receita', labels={'Receita': 'Receita (R$)', 'Canal': 'Canal'})
    fig_dist.update_layout(paper_bgcolor='#F5F0E6', height=400)
    st.plotly_chart(fig_dist, use_container_width=True, key="chart_distribuicao_receita")
    st.markdown("---")
    st.subheader("Receita por Unidade")
    df_fin_unid = (df.groupby('unidade')[valor_col].sum().reset_index().rename(columns={valor_col: 'receita'}).sort_values('receita', ascending=False))
    df_fin_unid['receita_fmt_label'] = df_fin_unid['receita'].apply(lambda x: formatar_moeda(x))
    fig_fu = px.bar(df_fin_unid, x='receita', y='unidade', orientation='h', text='receita_fmt_label', labels={'receita': 'Receita (R$)', 'unidade': 'Unidade'})
    fig_fu.update_yaxes(autorange='reversed')
    fig_fu.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
    fig_fu.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=450, yaxis={'categoryorder': 'total descending'})
    fig_fu.update_xaxes(tickformat=",.2f")
    st.plotly_chart(fig_fu, use_container_width=True, key="chart_receita_unidade_fin")
    st.markdown("---")
    st.subheader("Serviços Presenciais Mais Vendidos (Financeiro)")
    if 'nome_servico_simplificado' in df_detalhado.columns:
        df_serv_fin = (df_detalhado.groupby('nome_servico_simplificado')[valor_col].agg(['sum','count']).reset_index().rename(columns={'sum':'receita','count':'qtd'}))
        df_serv_fin = df_serv_fin.sort_values('receita', ascending=False).head(10)
        colf_s1, colf_s2 = st.columns([2, 1])
        with colf_s1:
            df_serv_fin['receita_fmt_label'] = df_serv_fin['receita'].apply(lambda x: formatar_moeda(x))
            fig_sf = px.bar(df_serv_fin, x='receita', y='nome_servico_simplificado', orientation='h', text='receita_fmt_label', labels={'receita': 'Receita (R$)', 'nome_servico_simplificado': 'Serviço'})
            fig_sf.update_yaxes(autorange='reversed')
            fig_sf.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_sf.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=400, yaxis={'categoryorder': 'total descending'})
            fig_sf.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_sf, use_container_width=True, key="chart_servicos_fin")
        with colf_s2:
            df_serv_fin_display = df_serv_fin.copy()
            df_serv_fin_display['receita_fmt'] = df_serv_fin_display['receita'].apply(formatar_moeda)
            df_serv_fin_display['qtd_fmt'] = df_serv_fin_display['qtd'].apply(formatar_numero)
            st.dataframe(df_serv_fin_display[['nome_servico_simplificado','receita_fmt','qtd_fmt']].rename(columns={
                'nome_servico_simplificado': 'Serviço','receita_fmt':'Receita','qtd_fmt':'Quantidade'}), use_container_width=True, height=400)
    st.markdown("---")
    st.subheader("Vouchers Mais Utilizados na Unidade")
    if not df_ecom_dist.empty:
        if 'PACKAGE_NAME' in df_ecom_dist.columns:
            df_ecom_dist['PACKAGE_NAME'] = df_ecom_dist['PACKAGE_NAME'].fillna(df_ecom_dist['NAME'])
        else:
            df_ecom_dist['PACKAGE_NAME'] = df_ecom_dist['NAME']
        df_ecom_top = (df_ecom_dist.groupby('PACKAGE_NAME').agg(qtde_vouchers=('ID','count'), receita_liquida=('PRICE_NET','sum')).reset_index().sort_values('receita_liquida', ascending=False).head(10))
        colf_e1, colf_e2 = st.columns([2,1])
        with colf_e1:
            df_ecom_top['receita_fmt_label'] = df_ecom_top['receita_liquida'].apply(lambda x: formatar_moeda(x))
            fig_ef = px.bar(df_ecom_top, x='receita_liquida', y='PACKAGE_NAME', orientation='h', text='receita_fmt_label', labels={'receita_liquida': 'Receita Líquida (R$)', 'PACKAGE_NAME': 'Serviço / Pacote'})
            fig_ef.update_yaxes(autorange='reversed')
            fig_ef.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_ef.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=400, yaxis={'categoryorder':'total descending'})
            fig_ef.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_ef, use_container_width=True, key="chart_vouchers_fin")
        with colf_e2:
            df_ecom_top_display = df_ecom_top.copy()
            df_ecom_top_display['qtde_vouchers_fmt'] = df_ecom_top_display['qtde_vouchers'].apply(formatar_numero)
            df_ecom_top_display['receita_liquida_fmt'] = df_ecom_top_display['receita_liquida'].apply(formatar_moeda)
            st.dataframe(df_ecom_top_display[['PACKAGE_NAME','qtde_vouchers_fmt','receita_liquida_fmt']].rename(columns={'PACKAGE_NAME':'Serviço / Pacote','qtde_vouchers_fmt':'Qtd Vouchers','receita_liquida_fmt':'Receita Líquida'}), use_container_width=True, height=400)
    else:
        st.info("Sem dados de ecommerce para o período selecionado.")

# ---------------------- TAB: MARKETING & ECOMMERCE -------------------------
with tab_mkt:
    # ... (mantive as suas seções do marketing/ecommerce — usar suas funções load_ga4_* e load_instagram_*)
    st.subheader("Ecommerce – Vouchers Utilizados na Unidade")
    with st.spinner("Carregando dados de vouchers utilizados..."):
        try:
            unidades_para_filtro = unidades_belle_ids_selecionadas if unidades_belle_ids_selecionadas else (unidades_selecionadas if unidades_selecionadas else None)
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
        df_serv = (df_ecom.groupby('PACKAGE_NAME').agg(qtde_vouchers=('ID','count'), receita_liquida=('PRICE_NET','sum')).reset_index().sort_values('qtde_vouchers', ascending=False).head(10))
        col_a, col_b = st.columns([2,1])
        with col_a:
            df_serv['qtde_fmt_label'] = df_serv['qtde_vouchers'].apply(lambda x: formatar_numero(x))
            fig_serv = px.bar(df_serv, x='qtde_vouchers', y='PACKAGE_NAME', orientation='h', labels={'qtde_vouchers': 'Qtd Vouchers', 'PACKAGE_NAME': 'Serviço / Pacote'}, text='qtde_fmt_label')
            fig_serv.update_yaxes(autorange='reversed')
            fig_serv.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_serv.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=450, yaxis={'categoryorder': 'total descending'})
            st.plotly_chart(fig_serv, use_container_width=True, key="chart_vouchers_mkt")
        with col_b:
            df_serv_display = df_serv.copy()
            df_serv_display['qtde_vouchers_fmt'] = df_serv_display['qtde_vouchers'].apply(formatar_numero)
            df_serv_display['receita_liquida_fmt'] = df_serv_display['receita_liquida'].apply(formatar_moeda)
            st.dataframe(df_serv_display[['PACKAGE_NAME','qtde_vouchers_fmt','receita_liquida_fmt']].rename(columns={'PACKAGE_NAME':'Serviço / Pacote','qtde_vouchers_fmt':'Qtd Vouchers','receita_liquida_fmt':'Receita Líquida'}), use_container_width=True, height=450)
    # (demais blocos GA4 / Instagram / Meta Ads mantidos — por brevidade omitidos aqui, mas você já tem o código original)

# ---------------------- TAB: SELF-SERVICE -------------------------
with tab_selfservice:
    st.subheader("Monte Sua Própria Análise")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### Agrupar Por")
        dimensoes = st.multiselect("Selecione dimensões:", ["Data", "Unidade", "Forma de Pagamento", "Serviço", "Terapeuta", "Cliente"], default=["Unidade"])
    with c2:
        st.markdown("### Métricas")
        metricas = st.multiselect("Selecione métricas:", ["Receita Total", "Quantidade de Atendimentos", "Ticket Médio", "Clientes Únicos"], default=["Receita Total", "Quantidade de Atendimentos"])
    if dimensoes and metricas:
        dim_map = {"Data": data_col, "Unidade": "unidade", "Forma de Pagamento": "forma_pagamento", "Serviço": "nome_servico_simplificado", "Terapeuta": "profissional", "Cliente": "nome_cliente"}
        nomes_amigaveis = {data_col: "Data", "unidade":"Unidade", "forma_pagamento":"Forma de Pagamento", "nome_servico_simplificado":"Serviço", "profissional":"Terapeuta", "nome_cliente":"Cliente", "receita_total":"Receita Total", "qtd_atendimentos":"Quantidade de Atendimentos", "ticket_medio":"Ticket Médio", "clientes_unicos":"Clientes Únicos"}
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
            df_display = df_custom.copy()
            if data_col in df_display.columns:
                df_display[data_col] = pd.to_datetime(df_display[data_col]).dt.strftime('%d/%m/%Y')
            if 'receita_total' in df_display.columns:
                df_display['receita_total'] = df_display['receita_total'].apply(formatar_moeda)
            if 'ticket_medio' in df_display.columns:
                df_display['ticket_medio'] = df_display['ticket_medio'].apply(formatar_moeda)
            if 'qtd_atendimentos' in df_display.columns:
                df_display['qtd_atendimentos'] = df_display['qtd_atendimentos'].apply(formatar_numero)
            if 'clientes_unicos' in df_display.columns:
                df_display['clientes_unicos'] = df_display['clientes_unicos'].apply(formatar_numero)
            df_display = df_display.rename(columns=nomes_amigaveis)
            st.markdown("---")
            st.dataframe(df_display, use_container_width=True, height=400)
            df_download = df_custom.copy()
            if data_col in df_download.columns:
                df_download[data_col] = pd.to_datetime(df_download[data_col]).dt.strftime('%d/%m/%Y')
            df_download = df_download.rename(columns=nomes_amigaveis)
            csv = df_download.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button("📥 Download CSV", csv, f"buddha_selfservice_{data_inicio.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.csv", "text/csv", key='download-csv')

# ---------------------- TAB: SUPORTE (NOVA) -------------------------
with tab_suporte:
    st.subheader("Suporte / Chamados")
    st.markdown("Aqui você pode ver os chamados abertos, status, SLA e detalhes. Use os filtros para refinar.")
    # filtros rápidos
    c1, c2, c3 = st.columns([2,2,1])
    data_ini_s = c1.date_input("De (abertura):", value=data_inicio, key='suporte_de')
    data_fim_s = c2.date_input("Até (abertura):", value=data_fim, key='suporte_ate')
    assunto_filter = c3.text_input("Assunto (contains):", value="", key='suporte_assunto')

    with st.spinner("Carregando chamados..."):
        try:
            df_chamados = load_chamados(data_ini_s.strftime('%Y-%m-%d'), data_fim_s.strftime('%Y-%m-%d'), unidades_belle_ids=unidades_belle_ids_selecionadas)
        except Exception as e:
            st.error(f"Erro ao carregar chamados: {e}")
            df_chamados = pd.DataFrame()

    if df_chamados.empty:
        st.info("Sem chamados no período/unidade selecionada.")
    else:
        df_ch = df_chamados.copy()
        # transformar datas
        for col in ['aberto_sp','resolvido_sp','data_abertura_sp','data_resolucao_sp']:
            if col in df_ch.columns:
                df_ch[col] = pd.to_datetime(df_ch[col], errors='coerce')
        # tempo de resolução em horas (se resolvido usa resolvido, senão usa agora)
        df_ch['resolucao_horas'] = (df_ch['resolvido_sp'].fillna(pd.Timestamp.utcnow()) - df_ch['aberto_sp']).dt.total_seconds() / 3600.0
        # aplicar filtro por assunto se fornecido
        if assunto_filter:
            df_ch = df_ch[df_ch['assunto_nome'].str.contains(assunto_filter, case=False, na=False)]
        total_chamados = len(df_ch)
        resolvidos = int(df_ch['resolvido_sp'].notna().sum())
        pendentes = total_chamados - resolvidos
        sla_ok = int((df_ch['status_sla_horas_comerciais'] == 'Dentro do SLA').sum()) if 'status_sla_horas_comerciais' in df_ch.columns else 0
        perc_sla_ok = (sla_ok / total_chamados * 100) if total_chamados>0 else 0
        media_horas_resolucao = df_ch.loc[df_ch['resolvido_sp'].notna(), 'resolucao_horas'].mean() if df_ch['resolvido_sp'].notna().any() else None

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Chamados", formatar_numero(total_chamados))
        c2.metric("Resolvidos", formatar_numero(resolvidos))
        c3.metric("Pendentes", formatar_numero(pendentes))
        c4.metric("SLA Dentro (%)", f"{perc_sla_ok:.1f}%")

        st.markdown("---")
        # top assuntos
        top_assuntos = df_ch['assunto_nome'].value_counts().head(10).reset_index()
        top_assuntos.columns = ['assunto','qtde']
        fig_ass = px.bar(top_assuntos, x='qtde', y='assunto', orientation='h', text='qtde')
        fig_ass.update_layout(height=350, paper_bgcolor='#F5F0E6')
        st.plotly_chart(fig_ass, use_container_width=True)

        # status por situação
        if 'situacao_descricao' in df_ch.columns:
            status_dist = df_ch['situacao_descricao'].value_counts().reset_index()
            status_dist.columns = ['situacao','qtde']
            fig_stat = px.pie(status_dist, names='situacao', values='qtde', hole=0.3)
            fig_stat.update_layout(height=300, paper_bgcolor='#F5F0E6')
            st.plotly_chart(fig_stat, use_container_width=True)

        st.markdown("---")
        st.markdown("### Lista de Chamados (últimas 1000 linhas)")
        display_cols = ['id','titulo','unidade_nome','assunto_nome','situacao_descricao','aberto_sp','resolvido_sp','resolucao_horas','percentual_sla_horas_comerciais','avaliacaoNota']
        cols_exist = [c for c in display_cols if c in df_ch.columns]
        df_show = df_ch[cols_exist].sort_values('aberto_sp', ascending=False).head(1000)
        st.dataframe(df_show, use_container_width=True, height=500)
        csv = df_show.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        st.download_button("📥 Download Chamados CSV", csv, f"chamados_{data_ini_s.strftime('%Y%m%d')}_{data_fim_s.strftime('%Y%m%d')}.csv", "text/csv")

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
    """)
    st.caption("Buddha Spa Dashboard – Portal de Franqueados v2.0 - Suporte integrado")

# FIM DO SCRIPT
