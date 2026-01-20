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
# FUNÇÃO PARA ADICIONAR TOTALIZADOR EM TABELAS
# -----------------------------------------------------------------------------
def adicionar_totalizador(df, colunas_numericas):
    """
    Adiciona uma linha de totalizador ao final do dataframe.
    
    Args:
        df: DataFrame a ser totalizado
        colunas_numericas: Lista de colunas que devem ser somadas
    
    Returns:
        DataFrame com linha de total
    """
    if df.empty:
        return df
    
    df_total = df.copy()
    
    # Criar linha de total
    total_row = {}
    for col in df_total.columns:
        if col in colunas_numericas:
            # Somar valores numéricos (remover formatação se necessário)
            if df_total[col].dtype == 'object':
                # Se estiver formatado, tentar extrair número
                try:
                    valores = df_total[col].str.replace('R$ ', '').str.replace('.', '').str.replace(',', '.').astype(float)
                    total_row[col] = formatar_moeda(valores.sum())
                except:
                    total_row[col] = '-'
            else:
                total_row[col] = df_total[col].sum()
        elif col == df_total.columns[0]:
            # Primeira coluna recebe "TOTAL"
            total_row[col] = '**TOTAL**'
        else:
            total_row[col] = '-'
    
    # Adicionar linha de total
    df_com_total = pd.concat([df_total, pd.DataFrame([total_row])], ignore_index=True)
    
    return df_com_total

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
        st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTkFQZ7QiSmOpEWC_9Ndsuqx_-roUMRJJkCvw&s", width=200)
    
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
        tipo_item,
        SUM(valor_liquido) AS valor_liquido,
        SUM(valor_bruto) AS valor_bruto,
        COUNT(*) AS qtd_itens
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
        {filtro_unidade}
    GROUP BY id_venda, unidade, data_atendimento, nome_cliente, profissional, forma_pagamento, nome_servico_simplificado, tipo_item
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
        tipo_item,
        valor_liquido,
        valor_bruto
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
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
def load_omnichannel_vouchers(data_inicio, data_fim, unidades_filtro=None):
    client = get_bigquery_client()
    
    # Construir filtro de unidades usando AFILLIATION_NAME
    filtro_unidade = ""
    if unidades_filtro and len(unidades_filtro) > 0:
        unidades_nomes = []
        for u in unidades_filtro:
            nome_sem_prefixo = u.replace('buddha spa - ', '').title()
            unidades_nomes.append(nome_sem_prefixo)
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
        CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) AS AFILLIATION_ID,
        u.post_title AS AFILLIATION_NAME,
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
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_email' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Email,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_phone' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_Phone,
        (SELECT MAX(CASE WHEN usermeta.meta_key = 'billing_cpf' THEN usermeta.meta_value END)
         FROM `buddha-bigdata.raw.wp_postmeta` pm 
         LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
         WHERE pm.meta_key = '_customer_user' AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_CPF,
        (SELECT MAX(CASE WHEN usermeta.meta_key = 'billing_cnpj' THEN usermeta.meta_value END)
         FROM `buddha-bigdata.raw.wp_postmeta` pm 
         LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
         WHERE pm.meta_key = '_customer_user' AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_CNPJ,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_City,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)
        ) AS Customer_State
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    INNER JOIN `buddha-bigdata.raw.wp_posts` u 
        ON u.post_type = 'unidade' 
        AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
        AND s.NAME LIKE CONCAT('% - ', u.post_title, '%')
    WHERE 
        s.CREATED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.CREATED_DATE <= TIMESTAMP('{data_fim} 23:59:59', 'America/Sao_Paulo')
        AND (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3')
        AND s.AFILLIATION_ID IS NOT NULL
        AND s.NAME NOT LIKE '%Voucher Experience%'
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

# Separar serviços e produtos
df_servicos = df[df['tipo_item'] == 'Serviço'].copy()
df_produtos = df[df['tipo_item'] == 'Produto'].copy()

# -----------------------------------------------------------------------------
# HEADER / KPIs
# -----------------------------------------------------------------------------
col_logo, col_title = st.columns([1, 5])

with col_logo:
    st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTkFQZ7QiSmOpEWC_9Ndsuqx_-roUMRJJkCvw&s", width=200)

with col_title:
    st.title("Buddha Spa - Dashboard de Unidades")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

# Calcular receitas separadas
receita_servicos = df_servicos[valor_col].sum()
receita_produtos = df_produtos[valor_col].sum()
receita_total = receita_servicos + receita_produtos

qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0

# Calcular ticket médio apenas com atendimentos que geraram receita
df_com_receita = df[df[valor_col] > 0]
qtd_atendimentos_pagos = int(df_com_receita['id_venda'].nunique())
ticket_medio = receita_total / qtd_atendimentos_pagos if qtd_atendimentos_pagos > 0 else 0

# KPIs PRINCIPAIS COM AJUDA
colk1, colk2, colk3, colk4 = st.columns(4)

with colk1:
    st.metric("Receita Total", formatar_moeda(receita_total))
    with st.popover("ℹ️"):
        st.caption("Soma de todos os valores líquidos dos atendimentos presenciais realizados no período selecionado (serviços + produtos).")

with colk2:
    st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
    with st.popover("ℹ️"):
        st.caption("Número total de atendimentos únicos realizados (cada ID de venda conta como um atendimento).")

with colk3:
    st.metric("Clientes Únicos", formatar_numero(qtd_clientes))
    with st.popover("ℹ️"):
        st.caption("Número de clientes distintos que foram atendidos no período. Um mesmo cliente pode ter feito múltiplos atendimentos.")

with colk4:
    st.metric("Ticket Médio por Atendimento", formatar_moeda(ticket_medio))
    with st.popover("ℹ️"):
        st.caption("Valor médio gasto por atendimento. Calculado como: Receita Total ÷ Quantidade de Atendimentos que geraram receita.")

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
    
    # NPS Score COM AJUDA
    col_titulo_nps, col_ajuda_nps = st.columns([0.97, 0.03])
    with col_titulo_nps:
        st.subheader("NPS - Net Promoter Score")
    with col_ajuda_nps:
        with st.popover("ℹ️"):
            st.markdown("""
            **O que é NPS?**
            
            Indicador de satisfação do cliente baseado na pergunta: 
            "De 0 a 10, quanto você recomendaria nossos serviços?"
            
            - **Promotores (9-10)**: Clientes entusiastas que vão recomendar
            - **Neutros (7-8)**: Clientes satisfeitos mas não entusiasmados
            - **Detratores (0-6)**: Clientes insatisfeitos
            
            **Cálculo:** (% Promotores - % Detratores)
            """)
    
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
        
        with col_nps1:
            st.metric("NPS Score", formatar_percentual(nps_score))
            with st.popover("ℹ️"):
                st.caption("Score geral calculado como (% Promotores - % Detratores). Varia de -100% a +100%.")
        
        with col_nps2:
            st.metric("Promotores", f"{promotores} ({formatar_percentual(perc_promotores)})")
            with st.popover("ℹ️"):
                st.caption("Clientes que deram notas 9 ou 10. São os mais propensos a recomendar o Buddha Spa.")
        
        with col_nps3:
            st.metric("Neutros", f"{neutros} ({formatar_percentual(perc_neutros)})")
            with st.popover("ℹ️"):
                st.caption("Clientes que deram notas 7 ou 8. Estão satisfeitos mas não entusiasmados.")
        
        with col_nps4:
            st.metric("Detratores", f"{detratores} ({formatar_percentual(perc_detratores)})")
            with st.popover("ℹ️"):
                st.caption("Clientes que deram notas de 0 a 6. Indicam insatisfação e risco de não retorno.")
        
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
        labels={valor_col: 'Receita (R$)', 'unidade': 'Unidade'},
        color_discrete_sequence=['#1f77b4']
    )
    fig_u.update_yaxes(autorange='reversed')
    fig_u.update_traces(textposition='inside', textfont=dict(color='white', size=11))
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
    
    if 'profissional' in df_servicos.columns:
        df_terap = (
            df_servicos.groupby(['unidade', 'profissional'])
            .agg(
                receita=(valor_col, 'sum'),
                qtd_atendimentos=('id_venda', 'nunique'),
                clientes_unicos=('nome_cliente', 'nunique') if 'nome_cliente' in df_servicos.columns else ('unidade', 'size')
            )
            .reset_index()
        )
        df_terap['ticket_medio'] = df_terap['receita'] / df_terap['qtd_atendimentos']
        df_terap = df_terap.sort_values('receita', ascending=False)
        
        # Gráficos separados por unidade com cores diferentes
        st.markdown("### Top Terapeutas por Receita (por Unidade)")
        st.markdown("Cada unidade abaixo mostra os terapeutas ordenados do maior para o menor (maior no topo).")
        
        # Determinar quais unidades mostrar
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
        
        # Palette de cores variadas
        palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', 
                   '#bcbd22', '#17becf', '#aec7e8', '#ffbb78', '#98df8a', '#ff9896', '#c5b0d5', '#c49c94',
                   '#f7b6d2', '#c7c7c7', '#dbdb8d', '#9edae5', '#393b79', '#637939', '#8c6d31', '#843c39']
        
        color_map = {}
        for i, u in enumerate(unidades_para_plot):
            color_map[u] = palette[i % len(palette)]
        
        # Loop por unidade
        for unidade in unidades_para_plot:
            st.markdown(f"#### {unidade.title()}")
            df_un = df_terap[df_terap['unidade'] == unidade].copy()
            if df_un.empty:
                st.info("Sem terapeutas registrados para essa unidade no período selecionado.")
                continue
            
            df_un = df_un.sort_values('receita', ascending=False).head(15)
            df_un['receita_fmt_label'] = df_un['receita'].apply(lambda x: formatar_moeda(x))
            
            fig_unit = px.bar(
                df_un,
                x='receita',
                y='profissional',
                orientation='h',
                text='receita_fmt_label',
                labels={'receita': 'Receita (R$)', 'profissional': 'Terapeuta'},
                color_discrete_sequence=[color_map[unidade]]
            )
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
        
        # Tabela de performance COM TOTALIZADOR
        st.markdown("### Tabela de Performance")
        df_terap_display = df_terap.copy()
        df_terap_display['receita'] = df_terap_display['receita'].apply(formatar_moeda)
        df_terap_display['qtd_atendimentos'] = df_terap_display['qtd_atendimentos'].apply(formatar_numero)
        df_terap_display['clientes_unicos'] = df_terap_display['clientes_unicos'].apply(formatar_numero)
        df_terap_display['ticket_medio'] = df_terap_display['ticket_medio'].apply(formatar_moeda)
        
        # Adicionar totalizador
        df_terap_com_total = adicionar_totalizador(
            df_terap_display,
            ['receita', 'qtd_atendimentos', 'clientes_unicos', 'ticket_medio']
        )
        
        st.dataframe(
            df_terap_com_total,
            use_container_width=True,
            height=500
        )
    
    st.markdown("---")
    
    st.subheader("Principais Serviços (Presencial)")
    
    if 'nome_servico_simplificado' in df_detalhado.columns:
        df_servicos_det = df_detalhado[df_detalhado['tipo_item'] == 'Serviço'].copy()
        
        df_servicos_agg = (
            df_servicos_det.groupby('nome_servico_simplificado')[valor_col]
            .agg(['sum', 'count'])
            .reset_index()
            .rename(columns={'sum': 'receita', 'count': 'qtd'})
        )
        df_servicos_agg['perc_receita'] = df_servicos_agg['receita'] / df_servicos_agg['receita'].sum()
        df_servicos_agg = df_servicos_agg.sort_values('receita', ascending=False).head(15)
        
        cols1, cols2 = st.columns([2, 1])
        
        with cols1:
            fig_s = px.bar(
                df_servicos_agg,
                x='receita',
                y='nome_servico_simplificado',
                orientation='h',
                text=df_servicos_agg['perc_receita'].map(lambda x: f"{x*100:.1f}%"),
                labels={'receita': 'Receita (R$)', 'nome_servico_simplificado': 'Serviço'},
                color_discrete_sequence=['#2ca02c']
            )
            fig_s.update_yaxes(autorange='reversed')
            fig_s.update_traces(textposition='inside', textfont=dict(color='white', size=11))
            fig_s.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=500,
                yaxis={'categoryorder': 'total descending'}
            )
            fig_s.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_s, use_container_width=True, key="chart_principais_servicos")
        
        with cols2:
            # Tabela COM TOTALIZADOR
            df_servicos_display = df_servicos_agg.copy()
            df_servicos_display['receita_fmt'] = df_servicos_display['receita'].apply(formatar_moeda)
            df_servicos_display['qtd_fmt'] = df_servicos_display['qtd'].apply(formatar_numero)
            df_servicos_display['perc_receita_fmt'] = df_servicos_display['perc_receita'].apply(lambda x: formatar_percentual(x*100))
            
            df_servicos_final = df_servicos_display[['nome_servico_simplificado', 'receita_fmt', 'qtd_fmt', 'perc_receita_fmt']].rename(columns={
                'nome_servico_simplificado': 'Serviço',
                'receita_fmt': 'Receita',
                'qtd_fmt': 'Quantidade',
                'perc_receita_fmt': '% Receita'
            })
            
            # Adicionar totalizador
            df_servicos_com_total = adicionar_totalizador(
                df_servicos_final,
                ['Receita', 'Quantidade']
            )
            
            st.dataframe(
                df_servicos_com_total,
                use_container_width=True,
                height=500
            )
    
    st.markdown("---")
    
    # GRÁFICO DE BARRAS 1: Atendimentos por Dia da Semana e Unidade
    col_titulo_bar1, col_ajuda_bar1 = st.columns([0.97, 0.03])
    with col_titulo_bar1:
        st.subheader("Atendimentos por Dia da Semana e Unidade")
    with col_ajuda_bar1:
        with st.popover("ℹ️"):
            st.caption("Gráfico de barras mostrando a quantidade de atendimentos por dia da semana em cada unidade.")
    
    df_bar1 = df_detalhado.copy()
    df_bar1['dia_semana'] = pd.to_datetime(df_bar1[data_col]).dt.day_name()
    
    dias_semana_map = {
        'Monday': 'Segunda-feira',
        'Tuesday': 'Terça-feira',
        'Wednesday': 'Quarta-feira',
        'Thursday': 'Quinta-feira',
        'Friday': 'Sexta-feira',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    df_bar1['dia_semana'] = df_bar1['dia_semana'].map(dias_semana_map)
    
    df_bar1_agg = (
        df_bar1.groupby(['dia_semana', 'unidade'])
        .size()
        .reset_index(name='qtd_atendimentos')
    )
    
    dias_ordem = ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']
    df_bar1_agg['dia_semana'] = pd.Categorical(df_bar1_agg['dia_semana'], categories=dias_ordem, ordered=True)
    df_bar1_agg = df_bar1_agg.sort_values('dia_semana')
    
    fig_bar1 = px.bar(
        df_bar1_agg,
        x='dia_semana',
        y='qtd_atendimentos',
        color='unidade',
        barmode='group',
        labels={'dia_semana': 'Dia da Semana', 'qtd_atendimentos': 'Quantidade de Atendimentos', 'unidade': 'Unidade'},
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    
    fig_bar1.update_layout(
        xaxis_title="Dia da Semana",
        yaxis_title="Quantidade de Atendimentos",
        height=450,
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5)
    )
    
    st.plotly_chart(fig_bar1, use_container_width=True, key="chart_bar_dia_semana_unidade")
    
    st.markdown("---")
    
    # GRÁFICO DE BARRAS 2: Atendimentos por Dia da Semana e Tipo de Serviço
    col_titulo_bar2, col_ajuda_bar2 = st.columns([0.97, 0.03])
    with col_titulo_bar2:
        st.subheader("Atendimentos por Dia da Semana e Tipo de Serviço")
    with col_ajuda_bar2:
        with st.popover("ℹ️"):
            st.caption("Gráfico de barras mostrando os 10 serviços mais populares e em quais dias da semana eles têm maior demanda.")
    
    if 'nome_servico_simplificado' in df_bar1.columns:
        top_servicos = (
            df_bar1.groupby('nome_servico_simplificado')
            .size()
            .sort_values(ascending=False)
            .head(10)
            .index.tolist()
        )
        
        df_bar2 = df_bar1[df_bar1['nome_servico_simplificado'].isin(top_servicos)]
        
        df_bar2_agg = (
            df_bar2.groupby(['dia_semana', 'nome_servico_simplificado'])
            .size()
            .reset_index(name='qtd_atendimentos')
        )
        
        df_bar2_agg['dia_semana'] = pd.Categorical(df_bar2_agg['dia_semana'], categories=dias_ordem, ordered=True)
        df_bar2_agg = df_bar2_agg.sort_values('dia_semana')
        
        fig_bar2 = px.bar(
            df_bar2_agg,
            x='dia_semana',
            y='qtd_atendimentos',
            color='nome_servico_simplificado',
            barmode='group',
            labels={'dia_semana': 'Dia da Semana', 'qtd_atendimentos': 'Quantidade de Atendimentos', 'nome_servico_simplificado': 'Serviço'},
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        
        fig_bar2.update_layout(
            xaxis_title="Dia da Semana",
            yaxis_title="Quantidade de Atendimentos",
            height=450,
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            legend=dict(orientation="h", yanchor="bottom", y=-0.5, xanchor="center", x=0.5)
        )
        
        st.plotly_chart(fig_bar2, use_container_width=True, key="chart_bar_dia_semana_servico")

# ---------------------- TAB: FINANCEIRO -------------------------
with tab_fin:
    st.subheader("Resumo Financeiro da Unidade")
    
    colf1, colf2, colf3 = st.columns(3)
    
    with colf1:
        st.metric("Receita Total (Atendimentos)", formatar_moeda(receita_total))
        with st.popover("ℹ️"):
            st.caption("Receita total dos atendimentos presenciais (serviços + produtos).")
    
    with colf2:
        st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
        with st.popover("ℹ️"):
            st.caption("Total de atendimentos únicos realizados.")
    
    with colf3:
        st.metric("Ticket Médio Unidade", formatar_moeda(ticket_medio))
        with st.popover("ℹ️"):
            st.caption("Valor médio por atendimento na unidade.")
    
    st.markdown("---")
    
    # EXPLICAÇÃO DA RECEITA TOTAL
    with st.expander("📊 Como calculamos a Receita Total?", expanded=False):
        st.markdown(f"""
        ### Composição da Receita Total: {formatar_moeda(receita_total)}
        
        A Receita Total é composta por todas as vendas de **serviços** e **produtos** realizadas na sua unidade durante o período selecionado.
        
        #### 🎯 O que está incluído:
        
        **Atendimentos Presenciais Pagos**
        - Todos os serviços realizados e pagos na unidade
        - Todos os produtos vendidos na unidade
        - Formas de pagamento: dinheiro, cartão, PIX, etc.
        - Apenas o **valor líquido** (já descontado impostos e taxas)
        
        #### 📋 Detalhamento:
        
        - **Período**: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
        - **Total de atendimentos**: {formatar_numero(qtd_atendimentos)}
        - **Clientes únicos**: {formatar_numero(qtd_clientes)}
        - **Ticket médio**: {formatar_moeda(ticket_medio)}
        
        #### ℹ️ Observações importantes:
        
        - **Vouchers do ecommerce**: Quando um voucher é **vendido** no site, a receita é da holding (ecommerce). Quando o voucher é **usado** na sua unidade, a holding faz o reembolso para a unidade.
        - **Parcerias e cupons**: Vendas realizadas através de parcerias aparecem separadamente na seção de detalhamento por origem.
        """)
    
    st.markdown("---")
    
    # Distribuição de Receita por Origem COM CORES DIFERENTES
    col_titulo_dist, col_ajuda_dist = st.columns([0.97, 0.03])
    with col_titulo_dist:
        st.subheader("Distribuição de Receita por Origem")
    with col_ajuda_dist:
        with st.popover("ℹ️"):
            st.markdown("""
            **Origens de Receita:**
            
            - **Belle (Sistema Local)**: Atendimentos pagos diretamente na unidade (serviços + produtos)
            - **Ecommerce (Vouchers)**: Vouchers comprados no site e **usados** na unidade (reembolsados pela holding)
            - **Parcerias (Cupons)**: Vendas através de cupons e parcerias
            
            O faturamento total é a soma de todas as origens.
            """)
    
    with st.spinner("Carregando dados de ecommerce..."):
        try:
            unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
            df_ecom_dist = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar ecommerce: {e}")
            df_ecom_dist = pd.DataFrame()
    
    # Calcular distribuição
    receita_belle = receita_total  # Serviços + Produtos do Belle
    receita_voucher = 0
    receita_parcerias = 0
    
    if not df_ecom_dist.empty:
        df_ecom_dist['PRICE_NET'] = pd.to_numeric(df_ecom_dist['PRICE_NET'], errors='coerce')
        receita_voucher = df_ecom_dist['PRICE_NET'].fillna(0).sum()
        
        # Identificar parcerias (se houver cupom)
        if 'COUPONS' in df_ecom_dist.columns:
            df_parcerias = df_ecom_dist[df_ecom_dist['COUPONS'].notna() & (df_ecom_dist['COUPONS'] != '')]
            receita_parcerias = df_parcerias['PRICE_NET'].fillna(0).sum()
    
    faturamento_total = receita_belle + receita_voucher
    
    # Cards de resumo
    cold1, cold2, cold3, cold4 = st.columns(4)
    
    with cold1:
        st.metric("Belle (Sistema Local)", formatar_moeda(receita_belle))
        with st.popover("ℹ️"):
            st.caption("Receita de atendimentos (serviços + produtos) pagos diretamente na unidade.")
    
    with cold2:
        st.metric("Ecommerce (Vouchers)", formatar_moeda(receita_voucher))
        with st.popover("ℹ️"):
            st.caption("Valor dos vouchers do ecommerce que foram **usados** na sua unidade (reembolsados pela holding).")
    
    with cold3:
        st.metric("Parcerias (Cupons)", formatar_moeda(receita_parcerias))
        with st.popover("ℹ️"):
            st.caption("Receita de vendas através de cupons e parcerias.")
    
    with cold4:
        st.metric("Faturamento Total", formatar_moeda(faturamento_total))
        with st.popover("ℹ️"):
            st.caption("Soma de todas as receitas: Belle + Vouchers + Parcerias.")
    
    st.markdown("---")
    
    # Gráficos de distribuição COM CORES DIFERENTES
    col_bar, col_pie = st.columns(2)
    
    with col_bar:
        st.markdown("#### Receita por Origem (Barras)")
        df_origem = pd.DataFrame({
            'Origem': ['Belle (Sistema Local)', 'Ecommerce (Vouchers)', 'Parcerias (Cupons)'],
            'Receita': [receita_belle, receita_voucher, receita_parcerias]
        })
        df_origem['Percentual'] = (df_origem['Receita'] / df_origem['Receita'].sum() * 100).round(1)
        
        fig_bar_origem = px.bar(
            df_origem,
            x='Origem',
            y='Receita',
            text=df_origem['Percentual'].apply(lambda x: f"{x}%"),
            labels={'Receita': 'Receita (R$)', 'Origem': 'Origem da Receita'},
            color='Origem',
            color_discrete_map={
                'Belle (Sistema Local)': '#8B0000',
                'Ecommerce (Vouchers)': '#FF6B35',
                'Parcerias (Cupons)': '#F7B801'
            }
        )
        fig_bar_origem.update_traces(textposition='outside')
        fig_bar_origem.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400,
            showlegend=False
        )
        fig_bar_origem.update_yaxes(tickformat=",.2f")
        st.plotly_chart(fig_bar_origem, use_container_width=True, key="chart_bar_origem")
    
    with col_pie:
        st.markdown("#### Distribuição Percentual")
        fig_pie_origem = px.pie(
            df_origem,
            names='Origem',
            values='Receita',
            color='Origem',
            color_discrete_map={
                'Belle (Sistema Local)': '#8B0000',
                'Ecommerce (Vouchers)': '#FF6B35',
                'Parcerias (Cupons)': '#F7B801'
            }
        )
        fig_pie_origem.update_traces(textposition='inside', textinfo='percent+label')
        fig_pie_origem.update_layout(paper_bgcolor='#F5F0E6', height=400)
        st.plotly_chart(fig_pie_origem, use_container_width=True, key="chart_pie_origem")
    
    st.markdown("---")
    
    # Tabela detalhada por origem COM TOTALIZADOR
    st.subheader("Detalhamento por Origem")
    
    df_detalhamento = pd.DataFrame({
        'Origem': ['Belle (Sistema Local)', 'Ecommerce (Vouchers)', 'Parcerias (Cupons)'],
        'Receita': [formatar_moeda(receita_belle), formatar_moeda(receita_voucher), formatar_moeda(receita_parcerias)],
        'Percentual': [
            formatar_percentual((receita_belle / faturamento_total * 100) if faturamento_total > 0 else 0),
            formatar_percentual((receita_voucher / faturamento_total * 100) if faturamento_total > 0 else 0),
            formatar_percentual((receita_parcerias / faturamento_total * 100) if faturamento_total > 0 else 0)
        ]
    })
    
    # Adicionar totalizador
    df_detalhamento_com_total = adicionar_totalizador(
        df_detalhamento,
        ['Receita']
    )
    
    st.dataframe(df_detalhamento_com_total, use_container_width=True)
    
    st.markdown("---")
    
    # Formas de Pagamento
    st.subheader("Distribuição por Forma de Pagamento")
    
    if 'forma_pagamento' in df.columns:
        df_pagamento = (
            df.groupby('forma_pagamento')[valor_col]
            .sum()
            .reset_index()
            .sort_values(valor_col, ascending=False)
        )
        df_pagamento['percentual'] = (df_pagamento[valor_col] / df_pagamento[valor_col].sum() * 100)
        
        col_pag1, col_pag2 = st.columns(2)
        
        with col_pag1:
            fig_pag = px.pie(
                df_pagamento,
                names='forma_pagamento',
                values=valor_col,
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            fig_pag.update_traces(textposition='inside', textinfo='percent+label')
            fig_pag.update_layout(paper_bgcolor='#F5F0E6', height=400)
            st.plotly_chart(fig_pag, use_container_width=True, key="chart_forma_pagamento")
        
        with col_pag2:
            # Tabela COM TOTALIZADOR
            df_pag_display = df_pagamento.copy()
            df_pag_display['receita_fmt'] = df_pag_display[valor_col].apply(formatar_moeda)
            df_pag_display['percentual_fmt'] = df_pag_display['percentual'].apply(formatar_percentual)
            
            df_pag_final = df_pag_display[['forma_pagamento', 'receita_fmt', 'percentual_fmt']].rename(columns={
                'forma_pagamento': 'Forma de Pagamento',
                'receita_fmt': 'Receita',
                'percentual_fmt': 'Percentual'
            })
            
            # Adicionar totalizador
            df_pag_com_total = adicionar_totalizador(
                df_pag_final,
                ['Receita']
            )
            
            st.dataframe(df_pag_com_total, use_container_width=True, height=400)

# ---------------------- TAB: MARKETING & ECOMMERCE -------------------------
with tab_mkt:
    st.subheader("Ecommerce - Vouchers da Unidade")
    
    st.info("📌 **Nota para franqueados**: Os dados de GA4, Instagram e Meta Ads são da holding e não estão disponíveis neste dashboard de unidades.")
    
    with st.spinner("Carregando dados de vouchers..."):
        try:
            unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
            df_vouchers = load_omnichannel_vouchers(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar vouchers: {e}")
            df_vouchers = pd.DataFrame()
    
    if not df_vouchers.empty:
        df_vouchers['PRICE_NET'] = pd.to_numeric(df_vouchers['PRICE_NET'], errors='coerce')
        
        total_vouchers = len(df_vouchers)
        receita_vouchers = df_vouchers['PRICE_NET'].fillna(0).sum()
        ticket_medio_voucher = receita_vouchers / total_vouchers if total_vouchers > 0 else 0
        
        colv1, colv2, colv3 = st.columns(3)
        
        with colv1:
            st.metric("Total de Vouchers Vendidos", formatar_numero(total_vouchers))
            with st.popover("ℹ️"):
                st.caption("Quantidade total de vouchers vendidos no ecommerce para sua unidade.")
        
        with colv2:
            st.metric("Receita de Vouchers", formatar_moeda(receita_vouchers))
            with st.popover("ℹ️"):
                st.caption("Valor total dos vouchers vendidos (receita do ecommerce).")
        
        with colv3:
            st.metric("Ticket Médio Voucher", formatar_moeda(ticket_medio_voucher))
            with st.popover("ℹ️"):
                st.caption("Valor médio por voucher vendido.")
        
        st.markdown("---")
        
        # Tabela de vouchers COM TOTALIZADOR
        st.markdown("### Vouchers Vendidos")
        
        df_vouchers_display = df_vouchers[['NAME', 'PACKAGE_NAME', 'CREATED_DATE_BRAZIL', 'PRICE_NET', 'STATUS', 'Customer_FullName']].copy()
        df_vouchers_display = df_vouchers_display.rename(columns={
            'NAME': 'Nome do Voucher',
            'PACKAGE_NAME': 'Pacote',
            'CREATED_DATE_BRAZIL': 'Data de Criação',
            'PRICE_NET': 'Valor',
            'STATUS': 'Status',
            'Customer_FullName': 'Cliente'
        })
        
        # Formatar valor
        df_vouchers_display['Valor'] = df_vouchers_display['Valor'].apply(formatar_moeda)
        
        # Mapear status
        status_map = {'1': 'Ativo', '2': 'Usado', '3': 'Expirado'}
        df_vouchers_display['Status'] = df_vouchers_display['Status'].map(status_map)
        
        # Adicionar totalizador
        df_vouchers_com_total = adicionar_totalizador(
            df_vouchers_display,
            ['Valor']
        )
        
        st.dataframe(df_vouchers_com_total, use_container_width=True, height=500)
    else:
        st.info("Sem dados de vouchers para o período selecionado.")

# ---------------------- TAB: SELF-SERVICE -------------------------
with tab_selfservice:
    st.subheader("Self-Service - Consultas Personalizadas")
    st.info("🚧 Seção em desenvolvimento. Em breve você poderá fazer consultas personalizadas aos seus dados.")

# ---------------------- TAB: AJUDA / GLOSSÁRIO -------------------------
with tab_gloss:
    st.subheader("Ajuda / Glossário")
    
    st.markdown("""
    ### 📚 Glossário de Termos
    
    #### Métricas Financeiras
    
    - **Receita Total**: Soma de todos os valores líquidos dos atendimentos (serviços + produtos) realizados no período.
    - **Valor Líquido**: Valor após descontar impostos e taxas.
    - **Valor Bruto**: Valor total antes de descontos.
    - **Ticket Médio**: Valor médio gasto por atendimento (Receita Total ÷ Quantidade de Atendimentos).
    - **Faturamento Total**: Soma de todas as receitas incluindo Belle, Vouchers e Parcerias.
    
    #### Origens de Receita
    
    - **Belle (Sistema Local)**: Receita de atendimentos (serviços + produtos) pagos diretamente na unidade através do sistema Belle.
    - **Ecommerce (Vouchers)**: Vouchers comprados no site e **usados** na unidade. A venda do voucher é receita da holding, mas quando usado na unidade, a holding faz o reembolso.
    - **Parcerias (Cupons)**: Vendas realizadas através de cupons de parceiros.
    
    #### Atendimento
    
    - **Atendimento**: Cada visita de um cliente à unidade (identificado por ID de venda único).
    - **Cliente Único**: Pessoa física que realizou pelo menos um atendimento no período.
    - **Profissional/Terapeuta**: Colaborador que realizou o atendimento.
    - **Serviço**: Tratamento ou terapia realizada (massagem, facial, etc.).
    - **Produto**: Item físico vendido na unidade.
    
    #### NPS (Net Promoter Score)
    
    - **NPS Score**: Indicador de satisfação calculado como (% Promotores - % Detratores).
    - **Promotores**: Clientes que deram notas 9 ou 10.
    - **Neutros**: Clientes que deram notas 7 ou 8.
    - **Detratores**: Clientes que deram notas de 0 a 6.
    
    #### Ecommerce
    
    - **Voucher**: Cupom digital vendido no site que pode ser usado na unidade.
    - **Status do Voucher**:
        - **Ativo**: Voucher vendido mas ainda não usado.
        - **Usado**: Voucher já utilizado na unidade.
        - **Expirado**: Voucher que passou da validade.
    
    ---
    
    ### ❓ Perguntas Frequentes
    
    **1. Por que a receita de vouchers aparece separada?**
    
    Quando um voucher é vendido no site, a receita é da holding (ecommerce). Quando o voucher é usado na sua unidade, a holding faz o reembolso para a unidade. Por isso separamos as duas receitas.
    
    **2. Como é calculado o ticket médio?**
    
    Ticket Médio = Receita Total ÷ Quantidade de Atendimentos que geraram receita (excluindo atendimentos gratuitos ou cortesias).
    
    **3. O que são "Clientes Únicos"?**
    
    É a quantidade de pessoas diferentes que foram atendidas no período. Um mesmo cliente pode ter feito vários atendimentos.
    
    **4. Por que não vejo dados de GA4, Instagram e Meta Ads?**
    
    Esses dados são da holding e não estão disponíveis no dashboard de unidades. Apenas administradores têm acesso a essas informações.
    
    **5. Como interpretar o NPS?**
    
    - **NPS > 75**: Excelente
    - **NPS 50-75**: Muito bom
    - **NPS 0-50**: Razoável
    - **NPS < 0**: Crítico (mais detratores que promotores)
    
    ---
    
    ### 📞 Suporte
    
    Em caso de dúvidas ou problemas técnicos, entre em contato com o suporte da Buddha Spa.
    """)
