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

def formatar_data(data):
    if pd.isna(data):
        return "-"
    try:
        if isinstance(data, str):
            data = pd.to_datetime(data)
        return data.strftime('%d/%m/%Y')
    except:
        return "-"

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
def load_info_unidades(unidades_filtro=None):
    """Carrega informações detalhadas das unidades da view unidades_view"""
    client = get_bigquery_client()
    
    filtro_unidade = ""
    if unidades_filtro and len(unidades_filtro) > 0:
        # Normaliza os nomes das unidades para comparação
        unidades_normalized = [u.lower().strip() for u in unidades_filtro]
        unidades_str = ','.join([f"'{nome}'" for nome in unidades_normalized])
        filtro_unidade = f"WHERE LOWER(TRIM(nome_fantasia)) IN ({unidades_str})"
    
    query = f"""
    SELECT 
        nome_fantasia,
        coordenador_comercial,
        quantidade_macas,
        cluster,
        banho,
        ayurvedica,
        data_inauguracao,
        cidade,
        uf,
        grupo_status,
        tipo_franqueado
    FROM `buddha-bigdata.analytics.unidades_view`
    {filtro_unidade}
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        st.error(f"Erro ao carregar informações das unidades: {e}")
        return pd.DataFrame()

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
with colk2:
    st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
with colk3:
    st.metric("Clientes Únicos", formatar_numero(qtd_clientes))
with colk4:
    st.metric("Ticket Médio por Atendimento", formatar_moeda(ticket_medio))

if is_admin and unidades_selecionadas:
    st.markdown("---")
    st.info(f"**📍 Unidades selecionadas:** {', '.join([u.title() for u in unidades_selecionadas])}")
elif not is_admin:
    st.markdown("---")
    st.info(f"**📍 Visualizando unidade:** {unidade_usuario.title()}")

st.divider()

tab_visao, tab_atend, tab_fin, tab_mkt, tab_selfservice, tab_gloss = st.tabs(
    ["Visão Geral", "Atendimento", "Financeiro", "Marketing & Ecommerce", "Self-Service", "Ajuda / Glossário"])

with tab_visao:
    st.subheader("📊 Informações das Unidades Selecionadas")
    
    # Carrega informações detalhadas das unidades
    with st.spinner("Carregando informações das unidades..."):
        df_info_unidades = load_info_unidades(unidades_selecionadas if is_admin else [unidade_usuario])
    
    if not df_info_unidades.empty:
        # Prepara os dados para exibição
        df_info_display = df_info_unidades.copy()
        
        # Formata as colunas
        df_info_display['data_inauguracao'] = df_info_display['data_inauguracao'].apply(formatar_data)
        df_info_display['quantidade_macas'] = df_info_display['quantidade_macas'].fillna(0).astype(int)
        df_info_display['banho'] = df_info_display['banho'].fillna(0).astype(int)
        df_info_display['ayurvedica'] = df_info_display['ayurvedica'].fillna(0).astype(int)
        
        # Renomeia as colunas
        df_info_display = df_info_display.rename(columns={
            'nome_fantasia': 'Unidade',
            'coordenador_comercial': 'Coordenador Comercial',
            'quantidade_macas': 'Nº Macas',
            'cluster': 'Cluster',
            'banho': 'Salas Banho',
            'ayurvedica': 'Salas Ayurvédica',
            'data_inauguracao': 'Data Inauguração',
            'cidade': 'Cidade',
            'uf': 'UF',
            'grupo_status': 'Status',
            'tipo_franqueado': 'Tipo'
        })
        
        # Seleciona apenas as colunas desejadas
        colunas_exibir = [
            'Unidade', 'Coordenador Comercial', 'Nº Macas', 'Cluster', 
            'Salas Banho', 'Salas Ayurvédica', 'Data Inauguração', 
            'Cidade', 'UF', 'Status', 'Tipo'
        ]
        
        df_info_display = df_info_display[colunas_exibir]
        
        # Exibe a tabela
        st.dataframe(df_info_display, use_container_width=True, height=400)
        
        # Cards com totais
        st.markdown("---")
        st.subheader("📈 Resumo das Unidades")
        
        col_r1, col_r2, col_r3, col_r4 = st.columns(4)
        
        total_macas = int(df_info_unidades['quantidade_macas'].fillna(0).sum())
        total_banho = int(df_info_unidades['banho'].fillna(0).sum())
        total_ayurvedica = int(df_info_unidades['ayurvedica'].fillna(0).sum())
        total_unidades = len(df_info_unidades)
        
        with col_r1:
            st.metric("Total de Unidades", formatar_numero(total_unidades))
        with col_r2:
            st.metric("Total de Macas", formatar_numero(total_macas))
        with col_r3:
            st.metric("Total Salas Banho", formatar_numero(total_banho))
        with col_r4:
            st.metric("Total Salas Ayurvédica", formatar_numero(total_ayurvedica))
        
        # Gráficos de distribuição
        st.markdown("---")
        col_g1, col_g2 = st.columns(2)
        
        with col_g1:
            st.markdown("#### Distribuição por Cluster")
            df_cluster = df_info_unidades.groupby('cluster').size().reset_index(name='quantidade')
            df_cluster = df_cluster.sort_values('quantidade', ascending=False)
            
            fig_cluster = px.bar(df_cluster, x='quantidade', y='cluster', orientation='h',
                                text='quantidade', labels={'quantidade': 'Quantidade', 'cluster': 'Cluster'})
            fig_cluster.update_yaxes(autorange='reversed')
            fig_cluster.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
            fig_cluster.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=350)
            st.plotly_chart(fig_cluster, use_container_width=True)
        
        with col_g2:
            st.markdown("#### Distribuição por Coordenador")
            df_coord = df_info_unidades.groupby('coordenador_comercial').size().reset_index(name='quantidade')
            df_coord = df_coord.sort_values('quantidade', ascending=False).head(10)
            
            fig_coord = px.bar(df_coord, x='quantidade', y='coordenador_comercial', orientation='h',
                              text='quantidade', labels={'quantidade': 'Quantidade', 'coordenador_comercial': 'Coordenador'})
            fig_coord.update_yaxes(autorange='reversed')
            fig_coord.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
            fig_coord.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=350)
            st.plotly_chart(fig_coord, use_container_width=True)
    
    else:
        st.warning("Nenhuma informação de unidade encontrada.")
    
    st.markdown("---")
    st.subheader("📈 Evolução da Receita")
    df_evolucao = df.groupby(data_col)[valor_col].sum().reset_index().sort_values(data_col)
    fig = px.line(df_evolucao, x=data_col, y=valor_col, markers=True, labels={valor_col: 'Receita (R$)', data_col: 'Data'})
    fig.update_layout(height=400, plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6')
    fig.update_traces(line_color='#8B0000')
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.subheader("💰 Receita por Unidade")
    df_unidades = df.groupby('unidade')[valor_col].sum().reset_index().sort_values(valor_col, ascending=False)
    df_unidades['receita_fmt'] = df_unidades[valor_col].apply(formatar_moeda)
    fig_u = px.bar(df_unidades, x=valor_col, y='unidade', orientation='h', text='receita_fmt',
                   labels={valor_col: 'Receita (R$)', 'unidade': 'Unidade'})
    fig_u.update_yaxes(autorange='reversed')
    fig_u.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
    fig_u.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=450)
    st.plotly_chart(fig_u, use_container_width=True)

with tab_atend:
    st.subheader("👥 Performance por Terapeuta")
    if 'profissional' in df.columns:
        df_terap = df.groupby(['unidade', 'profissional']).agg(
            receita=(valor_col, 'sum'),
            qtd_atendimentos=('id_venda', 'nunique'),
            clientes_unicos=('nome_cliente', 'nunique') if 'nome_cliente' in df.columns else ('unidade', 'size')
        ).reset_index()
        df_terap['ticket_medio'] = df_terap['receita'] / df_terap['qtd_atendimentos']
        df_terap = df_terap.sort_values('receita', ascending=False)
        
        df_terap_display = df_terap.copy()
        df_terap_display['receita'] = df_terap_display['receita'].apply(formatar_moeda)
        df_terap_display['qtd_atendimentos'] = df_terap_display['qtd_atendimentos'].apply(formatar_numero)
        df_terap_display['clientes_unicos'] = df_terap_display['clientes_unicos'].apply(formatar_numero)
        df_terap_display['ticket_medio'] = df_terap_display['ticket_medio'].apply(formatar_moeda)
        
        st.dataframe(df_terap_display, use_container_width=True, height=500)
    
    st.markdown("---")
    st.subheader("🛎️ Principais Serviços")
    if 'nome_servico_simplificado' in df_detalhado.columns:
        df_servicos = df_detalhado.groupby('nome_servico_simplificado')[valor_col].agg(['sum', 'count']).reset_index()
        df_servicos.columns = ['servico', 'receita', 'qtd']
        df_servicos['perc_receita'] = df_servicos['receita'] / df_servicos['receita'].sum() * 100
        df_servicos = df_servicos.sort_values('receita', ascending=False).head(15)
        
        col_s1, col_s2 = st.columns([2, 1])
        with col_s1:
            fig_s = px.bar(df_servicos, x='receita', y='servico', orientation='h',
                          text=df_servicos['perc_receita'].apply(lambda x: f"{x:.1f}%"),
                          labels={'receita': 'Receita (R$)', 'servico': 'Serviço'})
            fig_s.update_yaxes(autorange='reversed')
            fig_s.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
            fig_s.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=500)
            st.plotly_chart(fig_s, use_container_width=True)
        
        with col_s2:
            df_serv_display = df_servicos.copy()
            df_serv_display['receita'] = df_serv_display['receita'].apply(formatar_moeda)
            df_serv_display['qtd'] = df_serv_display['qtd'].apply(formatar_numero)
            df_serv_display['perc_receita'] = df_serv_display['perc_receita'].apply(lambda x: formatar_percentual(x))
            st.dataframe(df_serv_display[['servico', 'receita', 'qtd', 'perc_receita']].rename(columns={
                'servico': 'Serviço',
                'receita': 'Receita',
                'qtd': 'Quantidade',
                'perc_receita': '% Receita'
            }), use_container_width=True, height=500)

with tab_fin:
    st.subheader("💵 Resumo Financeiro")
    
    colf1, colf2, colf3 = st.columns(3)
    with colf1:
        st.metric("Receita Total (Atendimentos)", formatar_moeda(receita_belle))
    with colf2:
        st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
    with colf3:
        st.metric("Ticket Médio Unidade", formatar_moeda(ticket_medio))
    
    st.markdown("---")
    st.subheader("📊 Faturamento por Origem")
    
    faturamento_total = receita_belle + receita_ecommerce + receita_parceiro
    
    col_fat1, col_fat2, col_fat3, col_fat4 = st.columns(4)
    with col_fat1:
        st.metric("💰 Total", formatar_moeda(faturamento_total))
    with col_fat2:
        st.metric("🏪 Belle", formatar_moeda(receita_belle))
    with col_fat3:
        st.metric("🛒 Ecommerce", formatar_moeda(receita_ecommerce))
    with col_fat4:
        st.metric("🤝 Parcerias", formatar_moeda(receita_parceiro))
    
    df_fat = pd.DataFrame({
        'Origem': ['Belle', 'Ecommerce', 'Parcerias'],
        'Receita': [receita_belle, receita_ecommerce, receita_parceiro],
        'Percentual': [
            (receita_belle / faturamento_total * 100) if faturamento_total > 0 else 0,
            (receita_ecommerce / faturamento_total * 100) if faturamento_total > 0 else 0,
            (receita_parceiro / faturamento_total * 100) if faturamento_total > 0 else 0
        ]
    })
    
    col_g1, col_g2 = st.columns([2, 1])
    with col_g1:
        fig_fat = px.bar(df_fat, x='Origem', y='Receita', text=df_fat['Percentual'].apply(lambda x: f"{x:.1f}%"))
        fig_fat.update_traces(marker_color='#8B0000', textposition='outside')
        fig_fat.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=400)
        st.plotly_chart(fig_fat, use_container_width=True)
    
    with col_g2:
        fig_pie = px.pie(df_fat, names='Origem', values='Receita')
        fig_pie.update_layout(paper_bgcolor='#F5F0E6', height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

with tab_mkt:
    st.subheader("🛒 Marketing & Ecommerce")
    st.info("📌 **Nota**: Esta aba mostra apenas dados de ecommerce da sua unidade.")
    
    if not df_ecom_fat.empty:
        total_vouchers = len(df_ecom_fat)
        total_pedidos = int(df_ecom_fat['ORDER_ID'].nunique())
        receita_vouchers = df_ecom_fat['PRICE_NET'].fillna(0).sum()
        
        colm1, colm2, colm3 = st.columns(3)
        with colm1:
            st.metric("Total de Vouchers Utilizados", formatar_numero(total_vouchers))
        with colm2:
            st.metric("Total de Pedidos", formatar_numero(total_pedidos))
        with colm3:
            st.metric("Receita de Vouchers", formatar_moeda(receita_vouchers))
    else:
        st.warning("Sem dados de ecommerce para o período selecionado.")

with tab_selfservice:
    col_titulo_self, col_ajuda_self = st.columns([0.97, 0.03])
    with col_titulo_self:
        st.subheader("🔧 Monte Sua Própria Análise")
    with col_ajuda_self:
        with st.popover("ℹ️"):
            st.caption("Crie análises personalizadas selecionando dimensões e métricas. Ideal para relatórios específicos.")
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown("### Agrupar Por")
        dimensoes = st.multiselect(
            "Selecione dimensões:",
            ["Data", "Unidade", "Forma de Pagamento", "Serviço", "Terapeuta", "Cliente"],
            default=["Unidade"],
            key="selfservice_dimensoes"
        )
    
    with c2:
        st.markdown("### Métricas")
        metricas = st.multiselect(
            "Selecione métricas:",
            ["Receita Total", "Quantidade de Atendimentos", "Ticket Médio", "Clientes Únicos"],
            default=["Receita Total", "Quantidade de Atendimentos"],
            key="selfservice_metricas"
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
            st.markdown("### 📋 Resultado da Análise")
            st.dataframe(df_display, use_container_width=True, height=400)
            
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
        else:
            st.info("Selecione pelo menos uma dimensão válida.")
    else:
        st.info("👆 Selecione dimensões e métricas acima para criar sua análise personalizada.")

with tab_gloss:
    st.subheader("📚 Ajuda / Glossário de Métricas")
    
    st.markdown("""
    ### 📊 Principais Métricas
    
    **Receita Total** – Soma de todas as receitas: Belle (Sistema Local) + Ecommerce (Vouchers) + Parcerias (Cupons).
    
    **Receita Belle** – Soma de todos os valores líquidos de atendimentos presenciais no período.
    
    **Quantidade de Atendimentos** – Número de atendimentos únicos (`id_venda`).
    
    **Clientes Únicos** – Número de clientes distintos atendidos.
    
    **Ticket Médio por Atendimento** – Receita Belle ÷ Quantidade de Atendimentos que geraram receita.
    
    **NPS (Net Promoter Score)** – Indicador de satisfação calculado como: (% Promotores - % Detratores).
    - **Promotores**: Notas 9-10
    - **Neutros**: Notas 7-8
    - **Detratores**: Notas 0-6
    
    ---
    
    ### 🏢 Informações das Unidades
    
    **Coordenador Comercial** – Responsável comercial pela unidade.
    
    **Nº Macas** – Quantidade total de macas disponíveis na unidade.
    
    **Cluster** – Tipo/categoria da unidade (Rua, Shopping, Hotel, Academia, Store in Store, Quiosque).
    
    **Salas Banho** – Quantidade de salas com ofurô/banho disponíveis.
    
    **Salas Ayurvédica** – Quantidade de salas preparadas para tratamentos ayurvédicos.
    
    **Data Inauguração** – Data de abertura oficial da unidade.
    
    **Status** – Situação atual da unidade (Ativa, Onboarding, Implantação).
    
    **Tipo** – Classificação do franqueado (Mono ou Multi unidades).
    
    ---
    
    ### 🎫 Sobre Vouchers
    
    **Vouchers Utilizados:** Mostram apenas os vouchers que já foram utilizados na sua unidade (data: USED_DATE).
    
    **Diferença entre CREATED_DATE e USED_DATE:**
    - **CREATED_DATE**: Data em que o cliente comprou o voucher
    - **USED_DATE**: Data em que o cliente utilizou o voucher na unidade
    
    ---
    
    ### 💡 Dicas de Uso
    
    - Use os **ícones ℹ️** ao lado das métricas para ver explicações detalhadas
    - Na aba **Self-Service**, você pode criar análises personalizadas
    - Na aba **Financeiro**, veja o detalhamento completo por origem (Belle, Ecommerce, Parcerias)
    - Todas as tabelas com dados numéricos podem ser ordenadas clicando nos cabeçalhos
    - Na **Visão Geral**, você encontra informações operacionais completas das unidades
    
    ---
    
    ### 📚 Tabelas do BigQuery Utilizadas
    
    **1. Tabela de Atendimentos**
    - Nome: `buddha-bigdata.analytics.itens_atendimentos_analytics`
    - Campo de data: `data_atendimento`
    - Campo de receita: `valor_liquido`
    - Campo de unidade: `unidade`
    - Usado para: Atendimentos presenciais (Belle)
    
    **2. Tabela de Ecommerce**
    - Nome: `buddha-bigdata.raw.ecommerce_raw`
    - Data de uso: `USED_DATE`
    - Data de compra: `CREATED_DATE`
    - Unidade: `AFILLIATION_NAME`
    - Usado para: Vouchers vendidos online
    
    **3. Tabela de Unidades (View)**
    - Nome: `buddha-bigdata.analytics.unidades_view`
    - Campos principais: `coordenador_comercial`, `quantidade_macas`, `cluster`, `banho`, `ayurvedica`, `data_inauguracao`
    - Usado para: Informações operacionais e estruturais das unidades
    
    **4. Tabela de NPS**
    - Nome: `buddha-bigdata.analytics.analise_nps_analytics`
    - Campo de data: `data`
    - Classificações: `flag_promotor`, `flag_neutro`, `flag_detrator`
    - Usado para: Avaliações de satisfação
    
    ---
    
    ### 🔧 Suporte Técnico
    
    Em caso de dúvidas ou problemas com o dashboard:
    1. Verifique se as datas selecionadas estão corretas
    2. Confirme se a unidade tem dados no período
    3. Entre em contato com o suporte técnico
    
    ---
    
    ### 📖 Como Interpretar os Dados
    
    **Receita em Queda?**
    - Compare com o mesmo período do ano anterior
    - Verifique se houve feriados ou eventos especiais
    - Analise o NPS para identificar problemas de satisfação
    
    **Ticket Médio Baixo?**
    - Veja quais serviços estão sendo mais vendidos
    - Identifique oportunidades de upsell
    - Analise a performance dos terapeutas
    
    **Poucos Clientes Únicos?**
    - Foque em estratégias de retenção
    - Analise a origem dos vouchers (ecommerce vs presencial)
    - Verifique as avaliações de NPS
    
    **Infraestrutura da Unidade:**
    - Número de macas indica capacidade de atendimento simultâneo
    - Salas de banho e ayurvédica mostram diferenciais competitivos
    - Cluster e tipo de franqueado ajudam a entender o modelo de negócio
    """)
     
    st.caption("Buddha Spa Dashboard – Portal de Franqueados")
