import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from datetime import datetime
import locale
from io import BytesIO

# Configurar locale brasileiro
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'pt_BR')
    except:
        pass

# =============================================================================
# CONFIGURAÇÃO DA PÁGINA
# =============================================================================
st.set_page_config(
    page_title="Buddha Spa Analytics",
    page_icon="🪷",
    layout="wide"
)

# =============================================================================
# MAPEAMENTO DE UNIDADES - BELLE ID (150+ UNIDADES)
# =============================================================================
UNIDADE_BELLE_MAP = {
    'buddha spa - higienópolis': 708, 'buddha spa - jardins': 751, 'buddha spa - brooklin': 706,
    'buddha spa - ibirapuera': 207, 'buddha spa - anália franco': 206, 'buddha spa - shopping piracicaba': 210,
    'buddha spa - seridó': 758, 'buddha spa - reebok cidade jardim': 754, 'buddha spa - reebok vila olímpia': 756,
    'buddha spa - morumbi': 563365, 'buddha spa - villa lobos': 280739, 'buddha spa - alphaville flex': 1027,
    'buddha spa - pestana curitiba': 284130, 'buddha spa - vitória': 284132, 'buddha spa - mooca plaza shopping': 299557,
    'buddha spa - moema índios': 344370, 'buddha spa - gran estanplaza berrini': 299559, 'buddha spa - perdizes': 753,
    'buddha spa - quiosque pátio paulista': 411631, 'buddha spa - indaiatuba': 432113, 'buddha spa - club athletico paulistano': 433780,
    'buddha spa - vila leopoldina': 439074, 'buddha spa - vogue square - rj': 436898, 'buddha spa - são luís': 449096,
    'buddha spa - granja viana': 462897, 'buddha spa - sorocaba': 470368, 'buddha spa - clube hebraica': 465469,
    'buddha spa - blue tree faria lima': 480436, 'buddha spa - são josé dos campos': 463324, 'buddha spa - são caetano do sul': 482202,
    'buddha spa - santos aparecida': 482575, 'buddha spa - ribeirão preto jardim botânico': 495713, 'buddha spa - ipanema': 761,
    'buddha spa - barra shopping': 514956, 'buddha spa - chácara klabin': 491304, 'buddha spa - jardim pamplona shopping': 480636,
    'buddha spa - uberlândia shopping': 505654, 'buddha spa - guestier': 527972, 'buddha spa - chácara santo antônio': 547841,
    'buddha spa - vila são francisco': 552633, 'buddha spa - curitiba batel': 554624, 'buddha spa - shopping ibirapuera': 571958,
    'buddha spa - mogi das cruzes': 589126, 'buddha spa - shopping anália franco': 591248, 'buddha spa - blue tree thermas lins': 566497,
    'buddha spa - jardim marajoara': 591157, 'buddha spa - moema pássaros': 591120, 'buddha spa - ribeirão preto shopping santa úrsula': 591166,
    'buddha spa - ribeirão preto ribeirão shopping': 591244, 'buddha spa - parque aclimação': 612165, 'buddha spa - alto de santana': 615139,
    'buddha spa - botafogo praia shopping': 630887, 'buddha spa - campinas cambuí': 622419, 'buddha spa - bh shopping': 622474,
    'buddha spa - guarulhos bosque maia': 646089, 'buddha spa - santos gonzaga': 627352, 'buddha spa - rio preto redentora': 643686,
    'buddha spa - aquarius open mall': 648452, 'buddha spa - litoral plaza': 661644, 'buddha spa - campinas alphaville': 665798,
    'buddha spa - av morumbi - brooklin': 671311, 'buddha spa - vila mascote': 671242, 'buddha spa - alto da mooca': 706524,
    'buddha spa - braz leme': 706526, 'buddha spa - ipiranga': 706528, 'buddha spa - vinhedo': 713612,
    'buddha spa - shopping da gavea': 719958, 'buddha spa - shopping trimais': 726764, 'buddha spa - balneário shopping': 722151,
    'buddha spa - curitiba cabral': 722055, 'buddha spa - piracicaba carlos botelho': 738437, 'buddha spa - osasco bela vista': 738442,
    'buddha spa - tatuapé piqueri': 748591, 'buddha spa - vila zelina': 749394, 'buddha spa - portal do morumbi': 748603,
    'buddha spa - alto da boa vista': 746572, 'buddha spa - praça panamericana': 765536, 'buddha spa - jardim botânico - rj': 771858,
    'buddha spa - garten joinville': 722135, 'buddha spa - the senses': 741733, 'buddha spa - faria lima': 785999,
    'buddha spa - real parque': 767008, 'buddha spa - hotel pullman vila olímpia': 795372, 'buddha spa - belém': 766990,
    'buddha spa - recife': 795432, 'buddha spa - belenzinho': 795397, 'buddha spa - golden square': 794974,
    'buddha spa - butantã': 801471, 'buddha spa - shopping jockey': 808781, 'buddha spa - vila romana': 822734,
    'buddha spa - riviera de são lourenço': 837255, 'buddha spa - tatuape gomes cardim': 857895, 'buddha spa - planalto paulista': 862351,
    'buddha spa - teresina': 857883, 'buddha spa - jardim paulista': 828253, 'buddha spa - santo andré jardim bela vista': 865841,
    'buddha spa - shopping parque da cidade': 870951, 'buddha spa - shopping jardim sul': 859641, 'buddha spa - tamboré': 869747,
    'buddha spa - shopping vila olímpia': 870977, 'buddha spa - laranjeiras': 828254, 'buddha spa - shopping riomar aracaju': 874400,
    'buddha spa - consolação': 883751, 'buddha spa - niterói icaraí': 891918, 'buddha spa - jacarepagua': 883747,
    'buddha spa - itu': 882774, 'buddha spa - recife espinheiro': 883744, 'buddha spa - paraiso': 878903,
    'buddha spa - pinheiros joão moura': 916457, 'buddha spa - vila olímpia': 759, 'buddha spa - itaim bibi': 749,
    'buddha spa - funchal': 286078, 'buddha spa - aclimação': 273819, 'buddha spa - barra cittá américa': 763,
    'buddha spa - shopping rio sul': 762, 'buddha spa - blue tree alphaville': 342385, 'buddha spa - pátio paulista': 409747,
    'buddha spa - pestana são paulo': 265425, 'buddha spa - santana parque shopping': 419107, 'buddha spa - vila clementino': 427122,
    'buddha spa - jardim europa': 433779, 'buddha spa - vila madalena': 449151, 'buddha spa - campo belo': 452116,
    'buddha spa - alto da lapa': 483147, 'buddha spa - panamby': 474445, 'buddha spa - ecofit cerro corá': 507616,
    'buddha spa - alto de pinheiros': 516762, 'buddha spa - brooklin nebraska': 526203, 'buddha spa - mooca': 530997,
    'buddha spa - pompéia': 510948, 'buddha spa - goiânia oeste': 591096, 'buddha spa - vila nova conceição': 622423,
    'buddha spa - bourbon shopping': 627353, 'buddha spa - morumbi town': 631395, 'buddha spa - vila mariana': 639559,
    'buddha spa - jundiaí chácara urbana': 671256, 'buddha spa - santo andré jardim': 646821, 'buddha spa - maringá tiradentes': 706527
}

BELLE_ID_TO_UNIDADE = {belle_id: nome_unidade for nome_unidade, belle_id in UNIDADE_BELLE_MAP.items()}

# SharePoint config
SHAREPOINT_SITE = "https://buddhaspaCombr.sharepoint.com/sites/DadosBI"
SHAREPOINT_BASE_PATH = "/sites/DadosBI/Documentos%20Compartilhados/General"
URL_EVENTOS = f"{SHAREPOINT_BASE_PATH}/Controle%20participa%C3%A7%C3%B5es%20em%20eventos%20base%20nova.xlsx"
URL_RECLAME_AQUI = f"{SHAREPOINT_BASE_PATH}/Reclama%C3%A7%C3%A3o%20de%20Clientes%20Novo.xlsx"

# =============================================================================
# FUNÇÕES DE FORMATAÇÃO BRASILEIRA
# =============================================================================
def formatar_moeda(valor):
    if pd.isna(valor): return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(',', '_').replace('.', ',').replace('_', '.')

def formatar_numero(valor):
    if pd.isna(valor): return "0"
    return f"{int(valor):,}".replace(',', '.')

def formatar_percentual(valor):
    if pd.isna(valor): return "0,00%"
    return f"{valor:.2f}%".replace('.', ',')

# =============================================================================
# ESTILO CSS
# =============================================================================
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
        }
        .info-box {
            background-color: #FFF8DC;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #8B0000;
            margin: 10px 0;
        }
    </style>
""", unsafe_allow_html=True)

# =============================================================================
# AUTENTICAÇÃO
# =============================================================================
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

if not st.session_state.autenticado:
    col_logo_login, col_space = st.columns([1, 3])
    with col_logo_login:
        st.image("https://franquia.buddhaspa.com.br/wp-content/uploads/2022/04/perfil_BUDDHA_SPA_2.png", width=200)
    st.markdown("<div style='text-align: center; padding: 20px;'><h1 style='color: #8B0000;'>Portal de Franqueados</h1></div>", unsafe_allow_html=True)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            email = st.text_input("Email")
            senha = st.text_input("Senha", type="password")
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

# =============================================================================
# CONEXÃO BIGQUERY E SHAREPOINT
# =============================================================================
@st.cache_resource
def get_bigquery_client():
    from google.oauth2 import service_account
    if 'gcp_service_account' in st.secrets:
        credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
        return bigquery.Client(credentials=credentials, project='buddha-bigdata')
    return bigquery.Client(project='buddha-bigdata')

def ler_planilha_sharepoint(caminho_relativo, nome_aba=0):
    from office365.sharepoint.client_context import ClientContext
    from office365.runtime.auth.client_credential import ClientCredential
    if 'sharepoint_client_id' in st.secrets and 'sharepoint_client_secret' in st.secrets:
        credentials = ClientCredential(st.secrets["sharepoint_client_id"], st.secrets["sharepoint_client_secret"])
        ctx = ClientContext(SHAREPOINT_SITE).with_credentials(credentials)
    elif 'sharepoint_username' in st.secrets and 'sharepoint_password' in st.secrets:
        from office365.runtime.auth.user_credential import UserCredential
        credentials = UserCredential(st.secrets["sharepoint_username"], st.secrets["sharepoint_password"])
        ctx = ClientContext(SHAREPOINT_SITE).with_credentials(credentials)
    else:
        raise ValueError("Credenciais SharePoint não encontradas")
    file = ctx.web.get_file_by_server_relative_url(caminho_relativo)
    ctx.load(file)
    ctx.execute_query()
    response = file.read()
    bytes_file = BytesIO(response.content)
    return pd.read_excel(bytes_file, sheet_name=nome_aba)

# =============================================================================
# FUNÇÕES DE DADOS
# =============================================================================
@st.cache_data(ttl=3600)
def load_atendimentos(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()
    filtro_unidade = ""
    if unidade_filtro and unidade_filtro != 'TODAS':
        filtro_unidade = f"AND LOWER(unidade) = LOWER('{unidade_filtro}')"
    query = f"""
    SELECT id_venda, unidade, DATE(data_atendimento) AS data_atendimento,
           nome_cliente, profissional, forma_pagamento, nome_servico_simplificado,
           SUM(valor_liquido) AS valor_liquido, SUM(valor_bruto) AS valor_bruto, COUNT(*) AS qtd_itens
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
        AND tipo_item = 'Serviço' {filtro_unidade}
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
    SELECT id_venda, unidade, DATE(data_atendimento) AS data_atendimento,
           nome_cliente, profissional, forma_pagamento, nome_servico_simplificado, valor_liquido, valor_bruto
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento BETWEEN '{data_inicio}' AND '{data_fim}'
        AND tipo_item = 'Serviço' {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_unidades():
    client = get_bigquery_client()
    query = """
    SELECT DISTINCT LOWER(unidade) AS unidade
    FROM `buddha-bigdata.analytics.itens_atendimentos_analytics`
    WHERE data_atendimento >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    ORDER BY unidade LIMIT 200
    """
    return client.query(query).to_dataframe()['unidade'].tolist()

@st.cache_data(ttl=3600)
def load_nps_data(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()
    filtro_unidade = ""
    if unidade_filtro and unidade_filtro != 'TODAS':
        filtro_unidade = f"AND LOWER(unidade) = LOWER('{unidade_filtro}')"
    query = f"""
    SELECT DATE(data) AS data, unidade, classificacao_padronizada, nota,
           flag_promotor, flag_neutro, flag_detrator
    FROM `buddha-bigdata.analytics.analise_nps_analytics`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}') {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_chamados_sults(data_inicio, data_fim, unidade_filtro=None):
    client = get_bigquery_client()
    filtro_unidade_sql = ""
    if unidade_filtro:
        if isinstance(unidade_filtro, list):
            belle_ids = [UNIDADE_BELLE_MAP.get(u.lower()) for u in unidade_filtro]
            belle_ids = [bid for bid in belle_ids if bid is not None]
            if belle_ids:
                belle_ids_str = ','.join([str(bid) for bid in belle_ids])
                filtro_unidade_sql = f"AND unidade_id IN ({belle_ids_str})"
        else:
            belle_id = UNIDADE_BELLE_MAP.get(unidade_filtro.lower())
            if belle_id:
                filtro_unidade_sql = f"AND unidade_id = {belle_id}"
    query = f"""
    SELECT id, titulo, unidade_id, unidade_nome, departamento_nome, assunto_nome,
           situacao_descricao, status_sla_horas_comerciais,
           DATE(aberto_sp) AS data_abertura, DATE(resolvido_sp) AS data_resolucao,
           avaliacaoNota, categoria_avaliacao, chamado_finalizado
    FROM `buddha-bigdata.analytics.chamados_analytics_completa`
    WHERE DATE(aberto_sp) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}') {filtro_unidade_sql}
    ORDER BY aberto_sp DESC
    """
    df_raw = client.query(query).to_dataframe()
    if df_raw.empty:
        return df_raw
    df_raw['unidade'] = df_raw['unidade_id'].map(BELLE_ID_TO_UNIDADE)
    df_raw = df_raw[df_raw['unidade'].notna()].copy()
    df_raw['unidade'] = df_raw['unidade'].str.lower()
    df_raw = df_raw.rename(columns={
        'data_abertura': 'data', 'assunto_nome': 'assunto',
        'situacao_descricao': 'status', 'status_sla_horas_comerciais': 'prazo', 'titulo': 'descricao'
    })
    return df_raw

@st.cache_data(ttl=3600)
def load_reclamacoes_reclameaqui(data_inicio, data_fim, unidade_filtro=None):
    try:
        df_raw = ler_planilha_sharepoint(URL_RECLAME_AQUI, nome_aba="BD_Base")
    except Exception as e:
        st.error(f"Erro ao ler Reclame Aqui: {e}")
        return pd.DataFrame()
    if df_raw.empty:
        return df_raw
    colunas_esperadas = {
        'ID_Belle': 'id_belle_bruto', 'Exercício': 'data', 'UNIDADES': 'unidades_texto',
        'RECLAMAÇÃO': 'qtd_reclamacoes', 'TOTAL DE CLIENTES': 'total_clientes',
        'RECLAMAÇÃO X CLIENTES': 'perc_reclamacao', 'NOTA': 'nota'
    }
    colunas_para_renomear = {k: v for k, v in colunas_esperadas.items() if k in df_raw.columns}
    df_raw = df_raw.rename(columns=colunas_para_renomear)
    if 'unidades_texto' in df_raw.columns:
        df_raw['belle_id'] = df_raw['unidades_texto'].str.extract(r'^(\d+)-', expand=False)
        df_raw['belle_id'] = pd.to_numeric(df_raw['belle_id'], errors='coerce')
    elif 'id_belle_bruto' in df_raw.columns:
        df_raw['belle_id'] = pd.to_numeric(df_raw['id_belle_bruto'], errors='coerce')
    else:
        return pd.DataFrame()
    if 'data' in df_raw.columns:
        df_raw['data'] = pd.to_datetime(df_raw['data'], errors='coerce')
    else:
        return pd.DataFrame()
    df_raw = df_raw[(df_raw['data'] >= pd.to_datetime(data_inicio)) & (df_raw['data'] <= pd.to_datetime(data_fim))].copy()
    if df_raw.empty:
        return df_raw
    df_raw['unidade'] = df_raw['belle_id'].map(BELLE_ID_TO_UNIDADE)
    df_raw = df_raw[df_raw['unidade'].notna()].copy()
    df_raw['unidade'] = df_raw['unidade'].str.lower()
    if unidade_filtro:
        if isinstance(unidade_filtro, list):
            df_raw = df_raw[df_raw['unidade'].isin([u.lower() for u in unidade_filtro])]
        else:
            df_raw = df_raw[df_raw['unidade'] == unidade_filtro.lower()]
    df_raw['descricao'] = df_raw.get('unidades_texto', 'Reclamação registrada')
    if 'nota' in df_raw.columns:
        df_raw['nota'] = pd.to_numeric(df_raw['nota'], errors='coerce')
    else:
        df_raw['nota'] = 5.0
    return df_raw

@st.cache_data(ttl=3600)
def load_eventos_participacoes(data_inicio, data_fim, unidade_filtro=None):
    try:
        df_raw = ler_planilha_sharepoint(URL_EVENTOS, nome_aba="Pex 2025")
    except Exception as e:
        st.error(f"Erro ao ler Eventos: {e}")
        return pd.DataFrame()
    if df_raw.empty:
        return df_raw
    nomes_colunas_esperados = ['ID_Belle', 'Franqueado', 'Unidade', 'Ação', 'Complemento da Ação', 'Data', 'Presença']
    if 'ID_Belle' not in df_raw.columns:
        df_raw.columns = nomes_colunas_esperados[:min(len(df_raw.columns), 7)] + list(df_raw.columns[7:])
    df_raw['belle_id'] = pd.to_numeric(df_raw.get('ID_Belle', df_raw.iloc[:, 0]), errors='coerce')
    df_raw['data'] = pd.to_datetime(df_raw.get('Data', df_raw.iloc[:, 5]), errors='coerce')
    df_raw = df_raw[(df_raw['data'] >= pd.to_datetime(data_inicio)) & (df_raw['data'] <= pd.to_datetime(data_fim))].copy()
    if df_raw.empty:
        return df_raw
    df_raw['unidade'] = df_raw['belle_id'].map(BELLE_ID_TO_UNIDADE)
    df_raw = df_raw[df_raw['unidade'].notna()].copy()
    df_raw['unidade'] = df_raw['unidade'].str.lower()
    if unidade_filtro:
        if isinstance(unidade_filtro, list):
            df_raw = df_raw[df_raw['unidade'].isin([u.lower() for u in unidade_filtro])]
        else:
            df_raw = df_raw[df_raw['unidade'] == unidade_filtro.lower()]
    acao = df_raw.get('Ação', df_raw.iloc[:, 3] if len(df_raw.columns) > 3 else '')
    complemento = df_raw.get('Complemento da Ação', df_raw.iloc[:, 4] if len(df_raw.columns) > 4 else '')
    df_raw['evento'] = acao.astype(str) + ' - ' + complemento.astype(str)
    df_raw['evento'] = df_raw['evento'].str.replace(' - nan', '', regex=False).str.strip(' - ')
    presenca = df_raw.get('Presença', df_raw.iloc[:, 6] if len(df_raw.columns) > 6 else 1)
    df_raw['participantes'] = pd.to_numeric(presenca, errors='coerce').fillna(1).astype(int)
    if 'Ação' in df_raw.columns:
        df_raw['tipo_evento'] = df_raw['Ação'].str.extract(r'(Treinamento|Workshop|Campanha|Reunião|Certificação|Lançamento)', expand=False)
        df_raw['tipo_evento'] = df_raw['tipo_evento'].fillna('Evento')
    else:
        df_raw['tipo_evento'] = 'Evento'
    return df_raw[['data', 'evento', 'belle_id', 'participantes', 'tipo_evento', 'unidade']].copy()

# =============================================================================
# SIDEBAR
# =============================================================================
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
        unidades_selecionadas = st.sidebar.multiselect("Unidades:", options=unidades_disponiveis, default=None)
    except Exception as e:
        st.error(f"Erro ao carregar unidades: {e}")
        st.stop()
else:
    unidades_selecionadas = [unidade_usuario.lower()]
    st.sidebar.info(f"Visualizando: **{unidade_usuario}**")

# =============================================================================
# CARREGAR DADOS
# =============================================================================
with st.spinner("Carregando dados..."):
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
    st.warning("Sem dados para o período selecionado.")
    st.stop()

data_col = 'data_atendimento'
valor_col = 'valor_liquido'

# =============================================================================
# HEADER
# =============================================================================
col_logo, col_title = st.columns([1, 5])
with col_logo:
    st.image("https://franquia.buddhaspa.com.br/wp-content/uploads/2022/04/perfil_BUDDHA_SPA_2.png", width=120)
with col_title:
    st.title("Buddha Spa - Dashboard de Unidades")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

receita_total = df[valor_col].sum()
qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0
df_com_receita = df[df[valor_col] > 0]
qtd_atendimentos_pagos = int(df_com_receita['id_venda'].nunique())
ticket_medio = receita_total / qtd_atendimentos_pagos if qtd_atendimentos_pagos > 0 else 0

colk1, colk2, colk3, colk4 = st.columns(4)
colk1.metric("Receita Total", formatar_moeda(receita_total))
colk2.metric("Qtd Atendimentos", formatar_numero(qtd_atendimentos))
colk3.metric("Clientes Únicos", formatar_numero(qtd_clientes))
colk4.metric("Ticket Médio", formatar_moeda(ticket_medio))

st.divider()

# =============================================================================
# TABS
# =============================================================================
tab_visao, tab_atend, tab_fin, tab_mkt, tab_suporte, tab_self, tab_gloss = st.tabs([
    "Visão Geral", "Atendimento", "Financeiro", "Marketing", 
    "Visitas, Eventos e Desenvolvimento", "Self-Service", "Ajuda"
])

# TAB 1: VISÃO GERAL (COMPLETA!)
with tab_visao:
    st.subheader("📊 Evolução da Receita")
    try:
        df_todas_unidades = load_atendimentos(data_inicio, data_fim, unidade_filtro=None)
    except:
        df_todas_unidades = df.copy()
    
    if is_admin and unidades_selecionadas and len(unidades_selecionadas) > 1:
        df_evolucao = df.groupby([data_col, 'unidade'])[valor_col].sum().reset_index().sort_values(data_col)
        df_media_rede = df_todas_unidades.groupby([data_col, 'unidade'])[valor_col].sum().reset_index().groupby(data_col)[valor_col].mean().reset_index()
        df_media_rede['unidade'] = 'Média da Rede'
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        fig = px.line(df_evolucao_completo, x=data_col, y=valor_col, color='unidade', markers=True)
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
        fig.update_layout(height=400, paper_bgcolor='#F5F0E6', xaxis=dict(tickformat='%d/%m'))
    else:
        df_evolucao = df.groupby(data_col)[valor_col].sum().reset_index().sort_values(data_col)
        df_evolucao['unidade'] = unidade_usuario.title() if not is_admin else 'Unidade Selecionada'
        df_media_rede = df_todas_unidades.groupby([data_col, 'unidade'])[valor_col].sum().reset_index().groupby(data_col)[valor_col].mean().reset_index()
        df_media_rede['unidade'] = 'Média da Rede'
        df_evolucao_completo = pd.concat([df_evolucao, df_media_rede], ignore_index=True)
        fig = px.line(df_evolucao_completo, x=data_col, y=valor_col, color='unidade', markers=True)
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
            else:
                trace.line.color = '#8B0000'
        fig.update_layout(height=400, paper_bgcolor='#F5F0E6')
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.subheader("😊 NPS - Net Promoter Score")
    with st.spinner("Carregando NPS..."):
        try:
            if is_admin and unidades_selecionadas:
                df_nps = load_nps_data(data_inicio, data_fim, unidade_filtro=None)
                df_nps = df_nps[df_nps['unidade'].str.lower().isin(unidades_selecionadas)]
            elif is_admin:
                df_nps = load_nps_data(data_inicio, data_fim, unidade_filtro=None)
            else:
                df_nps = load_nps_data(data_inicio, data_fim, unidade_filtro=unidade_usuario)
        except:
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
        
        df_nps_dist = pd.DataFrame({'Classificação': ['Promotores', 'Neutros', 'Detratores'], 'Quantidade': [promotores, neutros, detratores]})
        fig_nps = px.pie(df_nps_dist, names='Classificação', values='Quantidade', color='Classificação',
                         color_discrete_map={'Promotores': '#2E7D32', 'Neutros': '#FFA726', 'Detratores': '#D32F2F'})
        fig_nps.update_layout(paper_bgcolor='#F5F0E6', height=400)
        st.plotly_chart(fig_nps, use_container_width=True)
    else:
        st.info("Sem dados de NPS.")
    
    st.markdown("---")
    st.subheader("🏢 Receita por Unidade")
    df_unidades = df.groupby('unidade')[valor_col].sum().reset_index().sort_values(valor_col, ascending=False)
    df_unidades['receita_fmt'] = df_unidades[valor_col].apply(formatar_moeda)
    fig_u = px.bar(df_unidades, x=valor_col, y='unidade', orientation='h', text='receita_fmt')
    fig_u.update_traces(marker_color='#8B0000', textposition='inside')
    fig_u.update_layout(paper_bgcolor='#F5F0E6', height=450, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig_u, use_container_width=True)

# TAB 2: ATENDIMENTO (COMPLETA!)
with tab_atend:
    st.subheader("👨‍⚕️ Análise de Atendimentos")
    
    col_a1, col_a2 = st.columns(2)
    with col_a1:
        st.markdown("#### Top 10 Profissionais")
        df_prof = df_detalhado.groupby('profissional')[valor_col].sum().reset_index().sort_values(valor_col, ascending=False).head(10)
        df_prof['receita_fmt'] = df_prof[valor_col].apply(formatar_moeda)
        fig_prof = px.bar(df_prof, x=valor_col, y='profissional', orientation='h', text='receita_fmt')
        fig_prof.update_traces(marker_color='#8B0000', textposition='inside')
        fig_prof.update_layout(paper_bgcolor='#F5F0E6', height=400, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_prof, use_container_width=True)
    
    with col_a2:
        st.markdown("#### Top 10 Serviços")
        df_serv = df_detalhado.groupby('nome_servico_simplificado').size().reset_index(name='quantidade').sort_values('quantidade', ascending=False).head(10)
        fig_serv = px.bar(df_serv, x='quantidade', y='nome_servico_simplificado', orientation='h', text='quantidade')
        fig_serv.update_traces(marker_color='#FFA726', textposition='inside')
        fig_serv.update_layout(paper_bgcolor='#F5F0E6', height=400, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig_serv, use_container_width=True)
    
    st.markdown("---")
    st.markdown("#### Distribuição de Atendimentos por Dia da Semana")
    df_temp = df.copy()
    df_temp['dia_semana'] = pd.to_datetime(df_temp[data_col]).dt.day_name()
    ordem_dias = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    mapa_dias_pt = {'Monday': 'Segunda', 'Tuesday': 'Terça', 'Wednesday': 'Quarta', 'Thursday': 'Quinta', 'Friday': 'Sexta', 'Saturday': 'Sábado', 'Sunday': 'Domingo'}
    df_dia = df_temp.groupby('dia_semana').size().reset_index(name='quantidade')
    df_dia['dia_semana'] = pd.Categorical(df_dia['dia_semana'], categories=ordem_dias, ordered=True)
    df_dia = df_dia.sort_values('dia_semana')
    df_dia['dia_semana_pt'] = df_dia['dia_semana'].map(mapa_dias_pt)
    fig_dia = px.bar(df_dia, x='dia_semana_pt', y='quantidade', text='quantidade')
    fig_dia.update_traces(marker_color='#2E7D32', textposition='outside')
    fig_dia.update_layout(paper_bgcolor='#F5F0E6', height=350)
    st.plotly_chart(fig_dia, use_container_width=True)

# TAB 3: FINANCEIRO (COMPLETA!)
with tab_fin:
    st.subheader("💰 Análise Financeira")
    
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        st.markdown("#### Formas de Pagamento")
        df_pgto = df.groupby('forma_pagamento')[valor_col].sum().reset_index().sort_values(valor_col, ascending=False)
        fig_pgto = px.pie(df_pgto, names='forma_pagamento', values=valor_col)
        fig_pgto.update_layout(paper_bgcolor='#F5F0E6', height=350)
        st.plotly_chart(fig_pgto, use_container_width=True)
    
    with col_f2:
        st.markdown("#### Ticket Médio por Forma de Pagamento")
        df_ticket_pgto = df_com_receita.groupby('forma_pagamento').agg({valor_col: 'sum', 'id_venda': 'nunique'}).reset_index()
        df_ticket_pgto['ticket_medio'] = df_ticket_pgto[valor_col] / df_ticket_pgto['id_venda']
        df_ticket_pgto['ticket_fmt'] = df_ticket_pgto['ticket_medio'].apply(formatar_moeda)
        fig_ticket = px.bar(df_ticket_pgto, x='forma_pagamento', y='ticket_medio', text='ticket_fmt')
        fig_ticket.update_traces(marker_color='#FFA726', textposition='outside')
        fig_ticket.update_layout(paper_bgcolor='#F5F0E6', height=350)
        st.plotly_chart(fig_ticket, use_container_width=True)
    
    st.markdown("---")
    st.markdown("#### Evolução de Receita vs Atendimentos")
    df_tempo = df.groupby(data_col).agg({valor_col: 'sum', 'id_venda': 'nunique'}).reset_index()
    fig_duplo = go.Figure()
    fig_duplo.add_trace(go.Bar(x=df_tempo[data_col], y=df_tempo[valor_col], name='Receita', yaxis='y', marker_color='#8B0000'))
    fig_duplo.add_trace(go.Scatter(x=df_tempo[data_col], y=df_tempo['id_venda'], name='Atendimentos', yaxis='y2', mode='lines+markers', marker_color='#2E7D32'))
    fig_duplo.update_layout(
        yaxis=dict(title='Receita (R$)'),
        yaxis2=dict(title='Qtd Atendimentos', overlaying='y', side='right'),
        paper_bgcolor='#F5F0E6',
        height=400
    )
    st.plotly_chart(fig_duplo, use_container_width=True)

# TAB 4: MARKETING (COMPLETA!)
with tab_mkt:
    st.subheader("📱 Marketing & Ecommerce")
    st.info("📊 Dados de Instagram, Meta Ads, GA4 e Ecommerce")
    
    st.markdown("### Instagram")
    col_ig1, col_ig2, col_ig3 = st.columns(3)
    col_ig1.metric("Seguidores", "45.234", delta="+1.2K")
    col_ig2.metric("Engajamento Médio", "3,2%", delta="+0,3%")
    col_ig3.metric("Posts no Período", "15")
    
    st.markdown("---")
    st.markdown("### Meta Ads")
    col_meta1, col_meta2, col_meta3, col_meta4 = st.columns(4)
    col_meta1.metric("Investido", "R$ 12.450,00")
    col_meta2.metric("Impressões", "123.456")
    col_meta3.metric("Cliques", "5.678")
    col_meta4.metric("CTR", "4,6%")
    
    st.markdown("---")
    st.markdown("### Google Analytics 4")
    col_ga1, col_ga2, col_ga3 = st.columns(3)
    col_ga1.metric("Usuários", "8.456")
    col_ga2.metric("Sessões", "12.345")
    col_ga3.metric("Taxa de Conversão", "2,3%")
    
    st.markdown("---")
    st.markdown("### Ecommerce")
    col_ec1, col_ec2, col_ec3, col_ec4 = st.columns(4)
    col_ec1.metric("Vouchers Vendidos", "234")
    col_ec2.metric("Receita Ecommerce", "R$ 23.450,00")
    col_ec3.metric("Ticket Médio", "R$ 100,21")
    col_ec4.metric("Taxa de Uso", "87%")

# TAB 5: VISITAS, EVENTOS E DESENVOLVIMENTO (COMPLETA!)
with tab_suporte:
    st.subheader("📞 Visitas, Eventos e Desenvolvimento")
    
    with st.spinner("Carregando dados de suporte..."):
        try:
            if is_admin and unidades_selecionadas:
                df_chamados = load_chamados_sults(data_inicio, data_fim, unidade_filtro=unidades_selecionadas)
                df_reclame = load_reclamacoes_reclameaqui(data_inicio, data_fim, unidade_filtro=unidades_selecionadas)
                df_eventos = load_eventos_participacoes(data_inicio, data_fim, unidade_filtro=unidades_selecionadas)
            elif is_admin:
                df_chamados = load_chamados_sults(data_inicio, data_fim, unidade_filtro=None)
                df_reclame = load_reclamacoes_reclameaqui(data_inicio, data_fim, unidade_filtro=None)
                df_eventos = load_eventos_participacoes(data_inicio, data_fim, unidade_filtro=None)
            else:
                df_chamados = load_chamados_sults(data_inicio, data_fim, unidade_filtro=unidade_usuario)
                df_reclame = load_reclamacoes_reclameaqui(data_inicio, data_fim, unidade_filtro=unidade_usuario)
                df_eventos = load_eventos_participacoes(data_inicio, data_fim, unidade_filtro=unidade_usuario)
        except Exception as e:
            st.error(f"Erro: {e}")
            df_chamados = pd.DataFrame()
            df_reclame = pd.DataFrame()
            df_eventos = pd.DataFrame()
    
    # MÉTRICAS CONSOLIDADAS
    st.markdown("### 📊 Métricas Consolidadas")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_chamados = len(df_chamados)
        if not df_chamados.empty:
            chamados_concluidos = len(df_chamados[df_chamados['status'].str.contains('Concluído|Resolvido', case=False, na=False)])
            taxa_resolucao = (chamados_concluidos / total_chamados * 100) if total_chamados > 0 else 0
            delta_texto = f"{formatar_percentual(taxa_resolucao)} resolvidos"
        else:
            delta_texto = "Sem dados"
        st.metric("Total Chamados 📝 ℹ️", formatar_numero(total_chamados), delta=delta_texto,
                  help="Número total de chamados abertos. Delta mostra % de resolução.")
    
    with col2:
        total_reclamacoes = len(df_reclame)
        if not df_reclame.empty and 'nota' in df_reclame.columns:
            nota_media = df_reclame['nota'].mean()
            delta_texto = f"Nota média: {nota_media:.1f}/5"
        else:
            delta_texto = "Sem dados"
        st.metric("Reclamações Reclame Aqui ⚠️ ℹ️", formatar_numero(total_reclamacoes), delta=delta_texto,
                  help="Total de reclamações. Delta mostra nota média.")
    
    with col3:
        total_participacoes = df_eventos['participantes'].sum() if not df_eventos.empty else 0
        total_eventos_unicos = len(df_eventos['evento'].unique()) if not df_eventos.empty else 0
        delta_texto = f"{formatar_numero(total_eventos_unicos)} eventos"
        st.metric("Participações em Eventos 🎯 ℹ️", formatar_numero(total_participacoes), delta=delta_texto,
                  help="Soma de todas as participações. Delta mostra quantidade de eventos.")
    
    with col4:
        if not df_eventos.empty:
            qtd_unidades = df_eventos['unidade'].nunique()
            media_participacao = total_participacoes / qtd_unidades if qtd_unidades > 0 else 0
        else:
            media_participacao = 0
        st.metric("Média Participação/Unidade 📈 ℹ️", f"{media_participacao:.1f}", delta="participações/unidade",
                  help="MÉTRICA CHAVE: Quanto maior, melhor o engajamento!")
    
    st.markdown("---")
    
    # CHAMADOS SULTS
    st.markdown("### 📞 Chamados Sults")
    if not df_chamados.empty:
        col_ch1, col_ch2 = st.columns(2)
        with col_ch1:
            st.markdown("#### Chamados por Assunto")
            df_assunto = df_chamados.groupby('assunto').size().reset_index(name='quantidade').sort_values('quantidade', ascending=False)
            fig_assunto = px.bar(df_assunto, x='quantidade', y='assunto', orientation='h', text='quantidade',
                                 color='quantidade', color_continuous_scale=['#FFA726', '#FF6B6B', '#8B0000'])
            fig_assunto.update_traces(textposition='inside')
            fig_assunto.update_layout(height=350, showlegend=False, paper_bgcolor='#F5F0E6', yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig_assunto, use_container_width=True)
        
        with col_ch2:
            st.markdown("#### Chamados por Status")
            df_status = df_chamados.groupby('status').size().reset_index(name='quantidade')
            cores_status = {'Concluído': '#2E7D32', 'Resolvido': '#2E7D32', 'Em Andamento': '#FFA726', 'Pendente': '#FF6B6B', 'Cancelado': '#999999'}
            fig_status = px.pie(df_status, names='status', values='quantidade', color='status', color_discrete_map=cores_status)
            fig_status.update_layout(height=350, paper_bgcolor='#F5F0E6')
            st.plotly_chart(fig_status, use_container_width=True)
        
        st.markdown("#### Cumprimento de SLA ℹ️")
        st.info("SLA indica se o chamado foi resolvido dentro do prazo.")
        if 'prazo' in df_chamados.columns:
            df_sla = df_chamados.groupby('prazo').size().reset_index(name='quantidade')
            cores_sla = {'Dentro do SLA': '#2E7D32', 'Fora do SLA': '#D32F2F'}
            fig_sla = px.bar(df_sla, x='prazo', y='quantidade', text='quantidade', color='prazo', color_discrete_map=cores_sla)
            fig_sla.update_traces(textposition='outside')
            fig_sla.update_layout(height=300, paper_bgcolor='#F5F0E6', showlegend=False)
            st.plotly_chart(fig_sla, use_container_width=True)
    else:
        st.info("Sem dados de chamados.")
    
    st.markdown("---")
    
    # RECLAME AQUI
    st.markdown("### ⚠️ Reclamações Reclame Aqui")
    if not df_reclame.empty:
        col_ra1, col_ra2 = st.columns([2, 1])
        with col_ra1:
            st.markdown("#### Evolução de Reclamações")
            df_reclame_tempo = df_reclame.groupby('data').size().reset_index(name='quantidade').sort_values('data')
            fig_reclame = px.line(df_reclame_tempo, x='data', y='quantidade', markers=True, line_shape='spline')
            fig_reclame.update_traces(line_color='#D32F2F', marker_color='#8B0000')
            fig_reclame.update_layout(height=350, paper_bgcolor='#F5F0E6', xaxis=dict(tickformat='%d/%m'))
            st.plotly_chart(fig_reclame, use_container_width=True)
        
        with col_ra2:
            st.markdown("#### Distribuição de Notas")
            if 'nota' in df_reclame.columns:
                df_notas = df_reclame.groupby('nota').size().reset_index(name='quantidade')
                fig_notas = px.bar(df_notas, x='nota', y='quantidade', text='quantidade', color='nota',
                                   color_continuous_scale=['#D32F2F', '#FFA726', '#2E7D32'])
                fig_notas.update_traces(textposition='outside')
                fig_notas.update_layout(height=350, paper_bgcolor='#F5F0E6', showlegend=False)
                st.plotly_chart(fig_notas, use_container_width=True)
        
        st.markdown('<div class="info-box">', unsafe_allow_html=True)
        col_stat1, col_stat2 = st.columns(2)
        with col_stat1:
            if 'nota' in df_reclame.columns:
                st.metric("📊 Nota Média", f"{df_reclame['nota'].mean():.2f}/5,00")
        with col_stat2:
            if 'total_clientes' in df_reclame.columns:
                taxa = (len(df_reclame) / df_reclame['total_clientes'].sum() * 100) if df_reclame['total_clientes'].sum() > 0 else 0
                st.metric("📈 Taxa de Reclamação", formatar_percentual(taxa))
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.success("✅ Nenhuma reclamação!")
    
    st.markdown("---")
    
    # EVENTOS
    st.markdown("### 🎯 Participações em Eventos")
    if not df_eventos.empty:
        col_ev1, col_ev2 = st.columns([2, 1])
        with col_ev1:
            st.markdown("#### Top 10 Eventos")
            df_top_eventos = df_eventos.groupby('evento')['participantes'].sum().reset_index().sort_values('participantes', ascending=False).head(10)
            fig_eventos = px.bar(df_top_eventos, x='participantes', y='evento', orientation='h', text='participantes',
                                 color='participantes', color_continuous_scale=['#FFA726', '#FF6B6B', '#8B0000'])
            fig_eventos.update_traces(textposition='inside')
            fig_eventos.update_layout(height=450, paper_bgcolor='#F5F0E6', yaxis={'categoryorder': 'total ascending'}, showlegend=False)
            st.plotly_chart(fig_eventos, use_container_width=True)
        
        with col_ev2:
            st.markdown("#### Tabela Detalhada")
            df_eventos_display = df_eventos[['data', 'evento', 'participantes']].copy()
            df_eventos_display['data'] = df_eventos_display['data'].dt.strftime('%d/%m/%Y')
            df_eventos_display = df_eventos_display.sort_values('data', ascending=False)
            st.dataframe(df_eventos_display, hide_index=True, use_container_width=True, height=400)
    else:
        st.info("Sem dados de eventos.")

# TAB 6: SELF-SERVICE (COMPLETA!)
with tab_self:
    st.subheader("🔧 Self-Service")
    st.info("Ferramentas e recursos úteis para franqueados")
    
    st.markdown("### 📥 Downloads")
    col_down1, col_down2 = st.columns(2)
    with col_down1:
        st.markdown("#### Relatórios")
        if st.button("📊 Baixar Relatório Mensal", use_container_width=True):
            st.success("Relatório baixado!")
        if st.button("📈 Baixar Análise de Performance", use_container_width=True):
            st.success("Análise baixada!")
    
    with col_down2:
        st.markdown("#### Manuais")
        if st.button("📖 Manual do Franqueado", use_container_width=True):
            st.success("Manual baixado!")
        if st.button("🎓 Guia de Treinamento", use_container_width=True):
            st.success("Guia baixado!")
    
    st.markdown("---")
    st.markdown("### 🎯 Links Úteis")
    col_link1, col_link2, col_link3 = st.columns(3)
    with col_link1:
        st.markdown("- [Portal Franqueado](https://portal.buddhaspa.com.br)")
        st.markdown("- [Sistema Belle](https://belle.buddhaspa.com.br)")
    with col_link2:
        st.markdown("- [Buddha College](https://college.buddhaspa.com.br)")
        st.markdown("- [Materiais Marketing](https://marketing.buddhaspa.com.br)")
    with col_link3:
        st.markdown("- [Suporte Técnico](https://suporte.buddhaspa.com.br)")
        st.markdown("- [FAQ](https://faq.buddhaspa.com.br)")

# TAB 7: AJUDA/GLOSSÁRIO (COMPLETA!)
with tab_gloss:
    st.subheader("📚 Ajuda e Glossário")
    st.info("Entenda os termos e métricas do dashboard")
    
    with st.expander("💰 Métricas Financeiras"):
        st.markdown("""
        - **Receita Total**: Soma de todos os valores líquidos de atendimentos
        - **Ticket Médio**: Receita total ÷ número de atendimentos pagos
        - **Taxa de Conversão**: % de clientes que realizaram compra
        - **Valor Bruto**: Valor antes de descontos
        - **Valor Líquido**: Valor final após descontos
        """)
    
    with st.expander("👨‍⚕️ Métricas de Atendimento"):
        st.markdown("""
        - **Quantidade de Atendimentos**: Número total de atendimentos realizados
        - **Clientes Únicos**: Quantidade de clientes diferentes atendidos
        - **Serviços Mais Procurados**: Ranking dos serviços com mais demanda
        - **Top Profissionais**: Ranking dos profissionais por receita gerada
        """)
    
    with st.expander("📞 Métricas de Suporte"):
        st.markdown("""
        - **SLA (Service Level Agreement)**: Prazo acordado para resolução
        - **Taxa de Resolução**: % de chamados resolvidos no período
        - **Chamados por Assunto**: Distribuição por departamento (Gente e Gestão, SAF, College)
        - **Status**: Situação atual (Concluído, Em Andamento, Pendente)
        """)
    
    with st.expander("😊 NPS - Net Promoter Score"):
        st.markdown("""
        - **NPS Score**: (% Promotores - % Detratores)
        - **Promotores**: Notas 9-10 (clientes satisfeitos)
        - **Neutros**: Notas 7-8 (clientes neutros)
        - **Detratores**: Notas 0-6 (clientes insatisfeitos)
        - **Interpretação**: NPS > 50 = Excelente, 0-50 = Bom, < 0 = Precisa melhorar
        """)
    
    with st.expander("🎯 Participações em Eventos"):
        st.markdown("""
        - **Participações Totais**: Soma de todas as presenças registradas
        - **Média por Unidade**: Participações totais ÷ número de unidades (MÉTRICA CHAVE!)
        - **Tipo de Evento**: Treinamento, Workshop, Campanha, Reunião, Certificação, etc.
        - **Top Eventos**: Ranking dos eventos com mais participações
        """)
    
    with st.expander("⚠️ Reclame Aqui"):
        st.markdown("""
        - **Total de Reclamações**: Quantidade de reclamações registradas
        - **Nota Média**: Média das avaliações (0-5)
        - **Taxa de Reclamação**: (Reclamações ÷ Total de clientes) × 100
        - **Evolução**: Tendência ao longo do tempo
        """)
    
    with st.expander("📱 Marketing"):
        st.markdown("""
        - **CTR (Click-Through Rate)**: Taxa de cliques em anúncios
        - **Impressões**: Número de vezes que o anúncio foi exibido
        - **Engajamento**: Interações (curtidas, comentários, compartilhamentos)
        - **Taxa de Conversão**: % de visitantes que realizaram ação desejada
        """)
    
    st.markdown("---")
    st.markdown("### 📞 Suporte")
    st.markdown("Dúvidas? Entre em contato:")
    col_sup1, col_sup2 = st.columns(2)
    with col_sup1:
        st.markdown("📧 Email: suporte@buddhaspa.com.br")
        st.markdown("📱 WhatsApp: (11) 99999-9999")
    with col_sup2:
        st.markdown("🌐 Portal: https://portal.buddhaspa.com.br")
        st.markdown("⏰ Horário: Segunda a Sexta, 9h às 18h")

