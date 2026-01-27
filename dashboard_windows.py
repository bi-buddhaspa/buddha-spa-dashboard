import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime
import locale

try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR')
    except:
        pass

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

def formatar_moeda(valor):
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')

def formatar_numero(valor):
    if pd.isna(valor):
        return "0"
    return f"{int(valor):,}".replace(',', '.')

def formatar_percentual(valor):
    if pd.isna(valor):
        return "0,00%"
    return f"{valor:.2f}%".replace('.', ',')

def adicionar_totalizador(df, colunas_numericas, primeira_coluna='', calcular_ticket_medio=False):
    if df.empty:
        return df
    total_row = {}
    for col in df.columns:
        if col in colunas_numericas:
            if df[col].dtype in ['int64', 'float64']:
                total_row[col] = df[col].sum()
            else:
                total_row[col] = ''
        elif col == 'ticket_medio' and calcular_ticket_medio:
            if 'receita' in df.columns and 'qtd_atendimentos' in df.columns:
                total_receita = df['receita'].sum()
                total_qtd = df['qtd_atendimentos'].sum()
                total_row[col] = total_receita / total_qtd if total_qtd > 0 else 0
            else:
                total_row[col] = ''
        else:
            if col == (primeira_coluna or df.columns[0]):
                total_row[col] = 'TOTAL'
            else:
                total_row[col] = ''
    df_com_total = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    return df_com_total

st.set_page_config(page_title="Buddha Spa Analytics", page_icon="🪷", layout="wide")

USUARIOS = {
    'joao.silva@buddhaspa.com.br': {'senha': '12345', 'nome': 'João Silva', 'unidade': 'buddha spa - higienópolis'},
    'leandro.santos@buddhaspa.com.br': {'senha': '625200', 'nome': 'Leandro Santos', 'unidade': 'TODAS'}
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
    st.markdown("<div style='text-align: center; padding: 20px;'><h1 style='color: #8B0000;'>Portal de Franqueados - Buddha Spa</h1><p style='color: #666;'>Faça login para acessar o dashboard</p></div>", unsafe_allow_html=True)
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

st.markdown("""
    <style>
        .stMetric {background-color: #F5F0E6; padding: 15px; border-radius: 10px; border: 2px solid #8B0000;}
        .stMetric label {color: #8B0000 !important; font-weight: bold; font-size: 0.9rem;}
        .stMetric [data-testid="stMetricValue"] {color: #2C1810; font-size: 1.5rem !important; font-weight: 600;}
        .stMetric [data-testid="stMetricDelta"] {font-size: 0.8rem;}
    </style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_bigquery_client():
    from google.oauth2 import service_account
    from google.cloud import bigquery
    if 'gcp_service_account' in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client(credentials=credentials, project='buddha-bigdata')
    else:
        return bigquery.Client(project='buddha-bigdata')

@st.cache_data(ttl=3600)
def load_atendimentos(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()
    filtro_unidade = ""
    if unidade_filtro and unidade_filtro != 'TODAS':
        filtro_unidade = f"AND LOWER(unidade) = LOWER('{unidade_filtro}')"
    query = f"""
    SELECT id_venda, unidade, DATE(data_atendimento) AS data_atendimento, nome_cliente, profissional, forma_pagamento,
           nome_servico_simplificado, SUM(valor_liquido) AS valor_liquido, SUM(valor_bruto) AS valor_bruto, COUNT(*) AS qtd_itens
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}' AND tipo_item = 'Serviço' {filtro_unidade}
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
    SELECT id_venda, unidade, DATE(data_atendimento) AS data_atendimento, nome_cliente, profissional, forma_pagamento,
           nome_servico_simplificado, valor_liquido, valor_bruto
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}' AND tipo_item = 'Serviço' {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_unidades(data_inicio=None, data_fim=None):
    client = get_bigquery_client()
    if data_inicio is None or data_fim is None:
        filtro_data = "1=1"
    else:
        filtro_data = f"data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'"
    query = f"""
    SELECT DISTINCT LOWER(unidade) AS unidade
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE {filtro_data} AND unidade IS NOT NULL AND unidade != ''
    ORDER BY unidade
    """
    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            st.warning("⚠️ Nenhuma unidade encontrada na base. Usando lista cadastrada.")
            return sorted(list(UNIDADE_BELLE_MAP.keys()))
        return df['unidade'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar unidades: {e}")
        return sorted(list(UNIDADE_BELLE_MAP.keys()))

@st.cache_data(ttl=3600)
def load_ecommerce_data(data_inicio, data_fim, unidades_filtro=None):
    client = get_bigquery_client()
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
    SELECT s.ID, s.NAME, s.STATUS, s.COUPONS, s.CREATED_DATE, DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
           s.USED_DATE, DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL, s.PRICE_NET, s.PRICE_GROSS, s.PRICE_REFOUND,
           s.KEY, s.ORDER_ID, (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
           u.post_title AS AFILLIATION_NAME,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_City,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END) FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_State
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    LEFT JOIN `buddha-bigdata.raw.wp_posts` u ON u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
    WHERE s.CREATED_DATE >= TIMESTAMP('2020-01-01 00:00:00', 'America/Sao_Paulo')
        AND s.USED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.USED_DATE <= TIMESTAMP('{data_fim} 23:59:59', 'America/Sao_Paulo')
        AND s.STATUS IN ('2','3') AND s.USED_DATE IS NOT NULL {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_omnichannel_vouchers(data_inicio, data_fim, unidades_filtro=None):
    client = get_bigquery_client()
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
    SELECT s.ID, s.NAME, s.STATUS, s.COUPONS, s.CREATED_DATE, DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
           s.USED_DATE, DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL, s.PRICE_NET, s.PRICE_GROSS, s.PRICE_REFOUND,
           s.KEY, s.ORDER_ID, (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
           CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) AS AFILLIATION_ID, u.post_title AS AFILLIATION_NAME,
           (SELECT CONCAT(MAX(CASE WHEN pm.meta_key = '_billing_first_name' THEN pm.meta_value END), ' ',
                          MAX(CASE WHEN pm.meta_key = '_billing_last_name' THEN pm.meta_value END))
            FROM `buddha-bigdata.raw.wp_posts` o LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_FullName,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_email' THEN pm.meta_value END) FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_Email,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_phone' THEN pm.meta_value END) FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_Phone,
           (SELECT MAX(CASE WHEN usermeta.meta_key = 'billing_cpf' THEN usermeta.meta_value END) FROM `buddha-bigdata.raw.wp_postmeta` pm
            LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
            WHERE pm.meta_key = '_customer_user' AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_CPF,
           (SELECT MAX(CASE WHEN usermeta.meta_key = 'billing_cnpj' THEN usermeta.meta_value END) FROM `buddha-bigdata.raw.wp_postmeta` pm
            LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
            WHERE pm.meta_key = '_customer_user' AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_CNPJ,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_City,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END) FROM `buddha-bigdata.raw.wp_posts` o
            LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_State
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    INNER JOIN `buddha-bigdata.raw.wp_posts` u ON u.post_type = 'unidade' AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
        AND s.NAME LIKE CONCAT('% - ', u.post_title, '%')
    WHERE s.CREATED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
        AND s.CREATED_DATE <= TIMESTAMP('{data_fim} 23:59:59', 'America/Sao_Paulo')
        AND (s.STATUS = '1' OR s.STATUS = '2' OR s.STATUS = '3') AND s.AFILLIATION_ID IS NOT NULL
        AND s.NAME NOT LIKE '%Voucher Experience%' {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_nps_data(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()
    filtro_unidade = ""
    if unidade_filtro and unidade_filtro != 'TODAS':
        filtro_unidade = f"AND LOWER(unidade) = LOWER('{unidade_filtro}')"
    query = f"""
    SELECT DATE(data) AS data, unidade, classificacao_padronizada, nota, flag_promotor, flag_neutro, flag_detrator
    FROM `buddha-bigdata.analytics.analise_nps_analytics`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}') {filtro_unidade}
    """
    return client.query(query).to_dataframe()

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
        unidades_disponiveis = load_unidades(data_inicio, data_fim)
        if not unidades_disponiveis:
            st.sidebar.warning("⚠️ Nenhuma unidade com dados no período selecionado")
            unidades_selecionadas = []
        else:
            unidades_selecionadas = st.sidebar.multiselect("Unidades:", options=unidades_disponiveis, default=None,
                help=f"Unidades com atendimentos entre {data_inicio.strftime('%d/%m/%Y')} e {data_fim.strftime('%d/%m/%Y')}")
    except Exception as e:
        st.sidebar.error(f"Erro ao carregar unidades: {e}")
        unidades_selecionadas = []
else:
    unidades_selecionadas = [unidade_usuario.lower()]
    st.sidebar.info(f"Você está visualizando apenas: **{unidade_usuario}**")

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

with st.spinner("Calculando faturamento total..."):
    try:
        unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
        df_ecom_fat = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
    except Exception as e:
        st.error(f"Erro ao carregar ecommerce: {e}")
        df_ecom_fat = pd.DataFrame()

receita_belle = df[valor_col].sum()
receita_ecommerce = 0
receita_parceiro = 0

if not df_ecom_fat.empty:
    df_ecom_fat['PRICE_NET'] = pd.to_numeric(df_ecom_fat['PRICE_NET'], errors='coerce')
    df_ecom_sem_cupom = df_ecom_fat[df_ecom_fat['COUPONS'].isna() | (df_ecom_fat['COUPONS'] == '')]
    df_ecom_com_cupom = df_ecom_fat[df_ecom_fat['COUPONS'].notna() & (df_ecom_fat['COUPONS'] != '')]
    receita_ecommerce = df_ecom_sem_cupom['PRICE_NET'].fillna(0).sum()
    receita_parceiro = df_ecom_com_cupom['PRICE_NET'].fillna(0).sum()

receita_total = receita_belle + receita_ecommerce + receita_parceiro

col_logo, col_title = st.columns([1, 5])
with col_logo:
    st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTkFQZ7QiSmOpEWC_9Ndsuqx_-roUMRJJkCvw&s", width=200)
with col_title:
    st.title("Buddha Spa - Dashboard de Unidades")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0
df_com_receita = df[df[valor_col] > 0]
qtd_atendimentos_pagos = int(df_com_receita['id_venda'].nunique())
ticket_medio = receita_belle / qtd_atendimentos_pagos if qtd_atendimentos_pagos > 0 else 0

colk1, colk2, colk3, colk4 = st.columns(4)
with colk1:
    st.metric("Receita Total", formatar_moeda(receita_total))
    with st.popover("ℹ️"):
        st.caption("Soma de todas as receitas: Belle (Sistema Local) + Ecommerce (Vouchers) + Parcerias (Cupons)")
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
        st.caption("Valor médio gasto por atendimento (Belle). Calculado como: Receita Belle ÷ Quantidade de Atendimentos que geraram receita.")

if is_admin and unidades_selecionadas:
    st.markdown("---")
    st.info(f"**📍 Unidades selecionadas:** {', '.join([u.title() for u in unidades_selecionadas])}")
elif not is_admin:
    st.markdown("---")
    st.info(f"**📍 Visualizando unidade:** {unidade_usuario.title()}")

with st.expander("📊 De onde vem a Receita Total?", expanded=False):
    st.markdown(f"""
    ### Como calculamos os **{formatar_moeda(receita_total)}**?
    A Receita Total é composta por **três origens** de faturamento:
    #### 💰 Composição da Receita Total:
    1. **🏪 Belle (Sistema Local): {formatar_moeda(receita_belle)}**
       - Atendimentos pagos diretamente na unidade
       - Formas de pagamento: dinheiro, cartão, PIX, etc.
    2. **🛒 Ecommerce (Vouchers): {formatar_moeda(receita_ecommerce)}**
       - Vouchers comprados online e utilizados na unidade
       - Sem cupons de desconto
    3. **🤝 Parcerias (Cupons): {formatar_moeda(receita_parceiro)}**
       - Vendas através de cupons de parceiros
       - Vouchers utilizados com desconto
    """)

st.divider()

tab_visao, tab_atend, tab_fin, tab_mkt, tab_selfservice, tab_gloss = st.tabs(
    ["Visão Geral", "Atendimento", "Financeiro", "Marketing & Ecommerce", "Self-Service", "Ajuda / Glossário"])

with tab_visao:
    st.subheader("Evolução da Receita")
    with st.spinner("Calculando média da rede..."):
        try:
            df_todas_unidades = load_atendimentos(data_inicio, data_fim, unidade_filtro=None)
        except:
            df_todas_unidades = df.copy()
    if is_admin and unidades_selecionadas and len(unidades_selecionadas) > 1:
        df_evolucao = df.groupby([data_col, 'unidade'])[valor_col].sum().reset_index().sort_values(data_col)
        df_media_rede = df_todas_unidades.groupby([data_col, 'unidade'])[valor_col].sum().reset_index().groupby(data_col)[valor_col].mean().reset_index()
        df_media_rede['unidade'] = 'Média da Rede'
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        fig = px.line(df_evolucao_completo, x=data_col, y=valor_col, color='unidade', markers=True, labels={valor_col: 'Receita (R$)', data_col: 'Data', 'unidade': 'Unidade'})
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
        fig.update_layout(xaxis_title="Data", yaxis_title="Receita (R$)", height=400, plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(tickformat='%d/%m', tickmode='auto', nticks=15, showgrid=True, gridcolor='lightgray'),
            yaxis=dict(showgrid=True, gridcolor='lightgray'))
    else:
        df_evolucao = df.groupby(data_col)[valor_col].sum().reset_index().sort_values(data_col)
        df_evolucao['unidade'] = unidade_usuario.title() if not is_admin else 'Unidade Selecionada'
        df_media_rede = df_todas_unidades.groupby([data_col, 'unidade'])[valor_col].sum().reset_index().groupby(data_col)[valor_col].mean().reset_index()
        df_media_rede['unidade'] = 'Média da Rede'
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        fig = px.line(df_evolucao_completo, x=data_col, y=valor_col, color='unidade', markers=True, labels={valor_col: 'Receita (R$)', data_col: 'Data', 'unidade': 'Unidade'})
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
            else:
                trace.line.color = '#8B0000'
        fig.update_layout(xaxis_title="Data", yaxis_title="Receita (R$)", height=400, plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6',
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            xaxis=dict(tickformat='%d/%m', tickmode='auto', nticks=15, showgrid=True, gridcolor='lightgray'),
            yaxis=dict(showgrid=True, gridcolor='lightgray'))
    fig.update_yaxes(tickformat=",.2f")
    st.plotly_chart(fig, use_container_width=True, key="chart_evolucao_receita")

with tab_atend:
    st.subheader("Performance por Terapeuta")
    if 'profissional' in df.columns:
        df_terap = df.groupby(['unidade', 'profissional']).agg(
            receita=(valor_col, 'sum'),
            qtd_atendimentos=('id_venda', 'nunique'),
            clientes_unicos=('nome_cliente', 'nunique') if 'nome_cliente' in df.columns else ('unidade', 'size')
        ).reset_index()
        df_terap['ticket_medio'] = df_terap['receita'] / df_terap['qtd_atendimentos']
        df_terap = df_terap.sort_values('receita', ascending=False)
        st.dataframe(df_terap, use_container_width=True, height=500)

with tab_fin:
    st.subheader("Resumo Financeiro da Unidade")
    colf1, colf2, colf3 = st.columns(3)
    with colf1:
        st.metric("Receita Total (Atendimentos)", formatar_moeda(receita_belle))
    with colf2:
        st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
    with colf3:
        st.metric("Ticket Médio Unidade", formatar_moeda(ticket_medio))

with tab_mkt:
    st.info("📌 **Nota para Franqueados**: Esta aba mostra apenas dados de ecommerce relacionados à sua unidade.")

with tab_selfservice:
    st.subheader("Monte Sua Própria Análise")
    st.info("Selecione dimensões e métricas para criar análises personalizadas")

with tab_gloss:
    st.subheader("Ajuda / Glossário de Métricas")
    st.markdown("""
    ### 📊 Principais Métricas
    **Receita Total** – Soma de todas as receitas: Belle + Ecommerce + Parcerias
    **Ticket Médio** – Receita Belle ÷ Quantidade de Atendimentos
    **NPS** – (% Promotores - % Detratores)
    """)
    st.caption("Buddha Spa Dashboard – Portal de Franqueados v3.1")
