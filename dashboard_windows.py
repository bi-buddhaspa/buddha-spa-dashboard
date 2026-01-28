"""
═══════════════════════════════════════════════════════════════════════════════
BUDDHA SPA - DASHBOARD DE FRANQUEADOS
Portal Analítico para Gestão de Unidades
═══════════════════════════════════════════════════════════════════════════════

VERSÃO: 3.4 - Com Mapeamento Correto de IDs
DATA: Janeiro 2026

CORREÇÃO: Agora usa ID_Belle, ID_Site e ID_Sults para fazer JOINs corretos
═══════════════════════════════════════════════════════════════════════════════
"""

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

# MAPEAMENTO DE IDs DAS UNIDADES
# ID_Belle -> ID usado no sistema Belle (atendimentos presenciais)
# ID_Site -> ID usado no WordPress/Ecommerce
# ID_Sults -> ID usado no sistema Sults (informações das unidades)
MAPEAMENTO_IDS = {
    708: {'id_site': 708, 'id_sults': 95, 'nome': 'Buddha SPA - Higienópolis'},
    759: {'id_site': 759, 'id_sults': 134, 'nome': 'Buddha SPA - Vila Olímpia'},
    749: {'id_site': 749, 'id_sults': 99, 'nome': 'Buddha SPA - Itaim Bibi'},
    751: {'id_site': 751, 'id_sults': 101, 'nome': 'Buddha SPA - Jardins'},
    706: {'id_site': 706, 'id_sults': 79, 'nome': 'Buddha SPA - Brooklin'},
    207: {'id_site': 207, 'id_sults': 96, 'nome': 'Buddha SPA - Ibirapuera'},
    206: {'id_site': 206, 'id_sults': 74, 'nome': 'Buddha SPA - Anália Franco'},
    210: {'id_site': 210, 'id_sults': 112, 'nome': 'Buddha SPA - Shopping Piracicaba'},
    758: {'id_site': 758, 'id_sults': 125, 'nome': 'Buddha SPA - Seridó'},
    754: {'id_site': 754, 'id_sults': 117, 'nome': 'Buddha SPA - Reebok Cidade Jardim'},
    756: {'id_site': 756, 'id_sults': 118, 'nome': 'Buddha SPA - Reebok Vila Olímpia'},
    563365: {'id_site': 563365, 'id_sults': 105, 'nome': 'Buddha SPA - Morumbi'},
    286078: {'id_site': 286078, 'id_sults': 91, 'nome': 'Buddha SPA - Funchal'},
    273819: {'id_site': 273819, 'id_sults': 71, 'nome': 'Buddha SPA - Aclimação'},
    280739: {'id_site': 280739, 'id_sults': 136, 'nome': 'Buddha SPA - Villa Lobos'},
    763: {'id_site': 763, 'id_sults': 75, 'nome': 'Buddha SPA - Barra Cittá América'},
    762: {'id_site': 762, 'id_sults': 120, 'nome': 'Buddha SPA - Shopping Rio Sul'},
    1027: {'id_site': 1027, 'id_sults': 90, 'nome': 'Buddha SPA - Alphaville Flex'},
    1000003: {'id_site': 1000003, 'id_sults': 139, 'nome': 'Buddha SPA - Bodytech Marista'},
    284130: {'id_site': 284130, 'id_sults': 110, 'nome': 'Buddha SPA - Pestana Curitiba'},
    284132: {'id_site': 284132, 'id_sults': 137, 'nome': 'Buddha SPA - Vitória'},
    299557: {'id_site': 299557, 'id_sults': 127, 'nome': 'Buddha SPA - Mooca Plaza Shopping'},
    344370: {'id_site': 344370, 'id_sults': 102, 'nome': 'Buddha SPA - Moema Índios'},
    299559: {'id_site': 299559, 'id_sults': 92, 'nome': 'Buddha SPA - Gran Estanplaza Berrini'},
    342385: {'id_site': 342385, 'id_sults': 77, 'nome': 'Buddha SPA - Blue Tree Alphaville'},
    409747: {'id_site': 409747, 'id_sults': 108, 'nome': 'Buddha SPA - Pátio Paulista'},
    753: {'id_site': 753, 'id_sults': 109, 'nome': 'Buddha SPA - Perdizes'},
    265425: {'id_site': 265425, 'id_sults': 111, 'nome': 'Buddha SPA - Pestana São Paulo'},
    411631: {'id_site': 411631, 'id_sults': 114, 'nome': 'Buddha SPA - Quiosque Pátio Paulista'},
    419107: {'id_site': 419107, 'id_sults': 128, 'nome': 'Buddha SPA - Santana Parque Shopping'},
    1000001: {'id_site': 1000001, 'id_sults': 89, 'nome': 'Buddha SPA - Espaço Be'},
    427122: {'id_site': 427122, 'id_sults': 131, 'nome': 'Buddha SPA - Vila Clementino'},
    432113: {'id_site': 432113, 'id_sults': 97, 'nome': 'Buddha SPA - Indaiatuba'},
    433780: {'id_site': 433780, 'id_sults': 85, 'nome': 'Buddha SPA - Club Athletico Paulistano'},
    433779: {'id_site': 433779, 'id_sults': 100, 'nome': 'Buddha SPA - Jardim Europa'},
    439074: {'id_site': 439074, 'id_sults': 132, 'nome': 'Buddha SPA - Vila Leopoldina'},
    436898: {'id_site': 436898, 'id_sults': 138, 'nome': 'Buddha SPA - Vogue Square - RJ'},
    449151: {'id_site': 449151, 'id_sults': 133, 'nome': 'Buddha SPA - Vila Madalena'},
    449096: {'id_site': 449096, 'id_sults': 124, 'nome': 'Buddha SPA - São Luís'},
    452116: {'id_site': 452116, 'id_sults': 81, 'nome': 'Buddha SPA - Campo Belo'},
    462897: {'id_site': 462897, 'id_sults': 93, 'nome': 'Buddha SPA - Granja Viana'},
    470368: {'id_site': 470368, 'id_sults': 129, 'nome': 'Buddha SPA - Sorocaba'},
    465469: {'id_site': 465469, 'id_sults': 84, 'nome': 'Buddha SPA - Clube Hebraica'},
    480436: {'id_site': 480436, 'id_sults': 78, 'nome': 'Buddha SPA - Blue Tree Faria Lima'},
    463324: {'id_site': 463324, 'id_sults': 123, 'nome': 'Buddha SPA - São José dos Campos'},
    483147: {'id_site': 483147, 'id_sults': 72, 'nome': 'Buddha SPA - Alto da Lapa'},
    474445: {'id_site': 474445, 'id_sults': 106, 'nome': 'Buddha SPA - Panamby'},
    482202: {'id_site': 482202, 'id_sults': 122, 'nome': 'Buddha SPA - São Caetano do Sul'},
    482575: {'id_site': 482575, 'id_sults': 121, 'nome': 'Buddha SPA - Santos Aparecida'},
    495713: {'id_site': 495713, 'id_sults': 116, 'nome': 'Buddha SPA - Ribeirão Preto Jardim Botânico'},
    761: {'id_site': 761, 'id_sults': 98, 'nome': 'Buddha SPA - Ipanema'},
    507616: {'id_site': 507616, 'id_sults': 88, 'nome': 'Buddha SPA - Ecofit Cerro Corá'},
    514956: {'id_site': 514956, 'id_sults': 76, 'nome': 'Buddha SPA - Barra Shopping'},
    491304: {'id_site': 491304, 'id_sults': 82, 'nome': 'Buddha SPA - Chácara Klabin'},
    480636: {'id_site': 480636, 'id_sults': 126, 'nome': 'Buddha SPA - Jardim Pamplona Shopping'},
    516762: {'id_site': 516762, 'id_sults': 73, 'nome': 'Buddha SPA - Alto de Pinheiros'},
    505654: {'id_site': 505654, 'id_sults': 130, 'nome': 'Buddha SPA - Uberlândia Shopping'},
    527972: {'id_site': 527972, 'id_sults': 94, 'nome': 'Buddha SPA - Guestier'},
    526203: {'id_site': 526203, 'id_sults': 80, 'nome': 'Buddha SPA - Brooklin Nebraska'},
    530997: {'id_site': 530997, 'id_sults': 103, 'nome': 'Buddha SPA - Mooca'},
    510948: {'id_site': 510948, 'id_sults': 113, 'nome': 'Buddha SPA - Pompéia'},
    547841: {'id_site': 547841, 'id_sults': 83, 'nome': 'Buddha SPA - Chácara Santo Antônio'},
    1000002: {'id_site': 1000002, 'id_sults': 115, 'nome': 'Buddha SPA - Quiosque SP Market'},
    552633: {'id_site': 552633, 'id_sults': 135, 'nome': 'Buddha SPA - Vila São Francisco'},
    554624: {'id_site': 554624, 'id_sults': 87, 'nome': 'Buddha SPA - Curitiba Batel'},
    560557: {'id_site': 560557, 'id_sults': 119, 'nome': 'Buddha SPA - Rio Design Leblon'},
    1000004: {'id_site': 1000004, 'id_sults': 107, 'nome': 'Buddha SPA - Paraíso'},
    571958: {'id_site': 571958, 'id_sults': 146, 'nome': 'Buddha SPA - Shopping Ibirapuera'},
    589126: {'id_site': 589126, 'id_sults': 141, 'nome': 'Buddha SPA - Mogi das Cruzes'},
    591096: {'id_site': 591096, 'id_sults': 148, 'nome': 'Buddha SPA - Goiânia Oeste'},
    591248: {'id_site': 591248, 'id_sults': 144, 'nome': 'Buddha SPA - Shopping Anália Franco'},
    566497: {'id_site': 566497, 'id_sults': 145, 'nome': 'Buddha SPA - Blue Tree Thermas Lins'},
    591157: {'id_site': 591157, 'id_sults': 150, 'nome': 'Buddha SPA - Jardim Marajoara'},
    591120: {'id_site': 591120, 'id_sults': 147, 'nome': 'Buddha SPA - Moema Pássaros'},
    591166: {'id_site': 591166, 'id_sults': 143, 'nome': 'Buddha SPA - Ribeirão Preto Shopping Santa Úrsula'},
    591244: {'id_site': 591244, 'id_sults': 142, 'nome': 'Buddha SPA - Ribeirão Preto Ribeirão Shopping'},
    612165: {'id_site': 612165, 'id_sults': 151, 'nome': 'Buddha Spa - Parque Aclimação'},
    615139: {'id_site': 615139, 'id_sults': 152, 'nome': 'Buddha Spa - Alto de Santana'},
    622423: {'id_site': 622423, 'id_sults': 154, 'nome': 'Buddha SPA - Vila Nova Conceição'},
    627353: {'id_site': 627353, 'id_sults': 155, 'nome': 'Buddha SPA - Bourbon Shopping'},
    630887: {'id_site': 630887, 'id_sults': 156, 'nome': 'Buddha SPA - Botafogo Praia Shopping'},
    622419: {'id_site': 622419, 'id_sults': 149, 'nome': 'Buddha SPA - Campinas Cambuí'},
    622474: {'id_site': 622474, 'id_sults': 157, 'nome': 'Buddha SPA - BH Shopping'},
    631395: {'id_site': 631395, 'id_sults': 104, 'nome': 'Buddha SPA - Morumbi Town'},
    639559: {'id_site': 639559, 'id_sults': 158, 'nome': 'Buddha SPA - Vila Mariana'},
    646089: {'id_site': 646089, 'id_sults': 163, 'nome': 'Buddha SPA - Guarulhos Bosque Maia'},
    627352: {'id_site': 627352, 'id_sults': 153, 'nome': 'Buddha SPA - Santos Gonzaga'},
    643686: {'id_site': 643686, 'id_sults': 160, 'nome': 'Buddha SPA - Rio Preto Redentora'},
    648452: {'id_site': 648452, 'id_sults': 159, 'nome': 'Buddha SPA - Aquarius Open Mall'},
    661644: {'id_site': 661644, 'id_sults': 166, 'nome': 'Buddha SPA - Litoral Plaza'},
    665798: {'id_site': 665798, 'id_sults': 165, 'nome': 'Buddha SPA - Campinas Alphaville'},
    671311: {'id_site': 671311, 'id_sults': 164, 'nome': 'Buddha SPA - AV Morumbi - Brooklin'},
    671242: {'id_site': 671242, 'id_sults': 168, 'nome': 'Buddha SPA - Vila Mascote'},
    671256: {'id_site': 671256, 'id_sults': 167, 'nome': 'Buddha SPA - Jundiaí Chácara Urbana'},
    706524: {'id_site': 706524, 'id_sults': 176, 'nome': 'Buddha SPA - Alto da Mooca'},
    706526: {'id_site': 706526, 'id_sults': 177, 'nome': 'Buddha SPA - Braz Leme'},
    646821: {'id_site': 646821, 'id_sults': 161, 'nome': 'Buddha SPA - Santo André Jardim'},
    706527: {'id_site': 706527, 'id_sults': 174, 'nome': 'Buddha SPA - Maringá Tiradentes'},
    706528: {'id_site': 706528, 'id_sults': 170, 'nome': 'Buddha SPA - Ipiranga'},
    713612: {'id_site': 713612, 'id_sults': 173, 'nome': 'Buddha SPA - Vinhedo'},
    719958: {'id_site': 719958, 'id_sults': 178, 'nome': 'Buddha SPA - Shopping da Gavea'},
    1000005: {'id_site': 1000005, 'id_sults': 162, 'nome': 'Buddha SPA - Quiosque Mogi Shopping'},
    726764: {'id_site': 726764, 'id_sults': 175, 'nome': 'Buddha SPA - Shopping TriMais'},
    722151: {'id_site': 722151, 'id_sults': 180, 'nome': 'Buddha SPA - Balneário Shopping'},
    722055: {'id_site': 722055, 'id_sults': 169, 'nome': 'Buddha SPA - Curitiba Cabral'},
    738437: {'id_site': 738437, 'id_sults': 172, 'nome': 'Buddha SPA - Piracicaba Carlos Botelho'},
    738442: {'id_site': 738442, 'id_sults': 179, 'nome': 'Buddha SPA - Osasco Bela Vista'},
    748591: {'id_site': 748591, 'id_sults': 182, 'nome': 'Buddha SPA - Tatuapé Piqueri'},
    749394: {'id_site': 749394, 'id_sults': 184, 'nome': 'Buddha SPA - Vila Zelina'},
    748603: {'id_site': 748603, 'id_sults': 185, 'nome': 'Buddha SPA - Portal do Morumbi'},
    746572: {'id_site': 746572, 'id_sults': 183, 'nome': 'Buddha SPA - Alto da Boa Vista'},
    765536: {'id_site': 765536, 'id_sults': 186, 'nome': 'Buddha Spa - Praça Panamericana'},
    771858: {'id_site': 771858, 'id_sults': 187, 'nome': 'Buddha Spa - Jardim Botânico - RJ'},
    722135: {'id_site': 722135, 'id_sults': 181, 'nome': 'Buddha Spa - Garten Joinville'},
    741733: {'id_site': 741733, 'id_sults': 171, 'nome': 'Buddha Spa - The Senses'},
    785999: {'id_site': 785999, 'id_sults': 192, 'nome': 'Buddha Spa - Faria Lima'},
    767008: {'id_site': 767008, 'id_sults': 188, 'nome': 'Buddha Spa - Real Parque'},
    795372: {'id_site': 795372, 'id_sults': 198, 'nome': 'Buddha Spa - Hotel Pullman Vila Olímpia'},
    766990: {'id_site': 766990, 'id_sults': 191, 'nome': 'Buddha Spa - Belém'},
    795432: {'id_site': 795432, 'id_sults': 194, 'nome': 'Buddha Spa - Recife'},
    795397: {'id_site': 795397, 'id_sults': 190, 'nome': 'Buddha Spa - Belenzinho'},
    794974: {'id_site': 794974, 'id_sults': 193, 'nome': 'Buddha Spa - Golden Square'},
    801471: {'id_site': 801471, 'id_sults': 196, 'nome': 'Buddha Spa - Butantã'},
    808781: {'id_site': 808781, 'id_sults': 195, 'nome': 'Buddha Spa - Shopping Jockey'},
    822734: {'id_site': 822734, 'id_sults': 197, 'nome': 'Buddha Spa - Vila Romana'},
    837255: {'id_site': 837255, 'id_sults': 200, 'nome': 'Buddha Spa - Riviera de São Lourenço'},
    857895: {'id_site': 857895, 'id_sults': 201, 'nome': 'Buddha Spa - Tatuape Gomes Cardim'},
    862351: {'id_site': 862351, 'id_sults': 199, 'nome': 'Buddha Spa - Planalto Paulista'},
    857883: {'id_site': 857883, 'id_sults': 204, 'nome': 'Buddha Spa - Teresina'},
    828253: {'id_site': 828253, 'id_sults': 208, 'nome': 'Buddha Spa - Jardim Paulista'},
    870977: {'id_site': 870977, 'id_sults': 215, 'nome': 'Buddha Spa Shopping Vila Olímpia'},
    865841: {'id_site': 865841, 'id_sults': 205, 'nome': 'Buddha Spa - Santo André Jardim Bela Vista'},
    870951: {'id_site': 870951, 'id_sults': None, 'nome': 'Buddha Spa - Shopping Parque da Cidade'},
    859641: {'id_site': 859641, 'id_sults': None, 'nome': 'Buddha Spa - Shopping Jardim Sul'},
    869747: {'id_site': 869747, 'id_sults': None, 'nome': 'Buddha Spa - Tamboré'},
    828254: {'id_site': 828254, 'id_sults': None, 'nome': 'Buddha Spa - Laranjeiras'},
    874400: {'id_site': 874400, 'id_sults': None, 'nome': 'Buddha Spa - Shopping Riomar Aracaju'},
    883751: {'id_site': 883751, 'id_sults': None, 'nome': 'Buddha Spa - Consolação'},
    891918: {'id_site': 891918, 'id_sults': None, 'nome': 'Buddha Spa - Niterói Icaraí'},
    883747: {'id_site': 883747, 'id_sults': None, 'nome': 'Buddha Spa - Jacarepagua'},
    882774: {'id_site': 882774, 'id_sults': None, 'nome': 'Buddha Spa - Itu'},
    883744: {'id_site': 883744, 'id_sults': None, 'nome': 'Buddha Spa - Recife Espinheiro'},
    878903: {'id_site': 878903, 'id_sults': None, 'nome': 'Buddha Spa - Paraiso'},
    916457: {'id_site': 916457, 'id_sults': None, 'nome': 'Buddha Spa - Pinheiros João Moura'},
}

# Criar dicionário reverso: nome_unidade_lower -> id_belle
UNIDADE_BELLE_MAP = {info['nome'].lower().replace('buddha spa - ', 'buddha spa - '): id_belle 
                     for id_belle, info in MAPEAMENTO_IDS.items()}

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
    """
    Carrega informações das unidades usando ID_Sults
    """
    client = get_bigquery_client()
    
    filtro_unidade = ""
    if unidades_filtro and len(unidades_filtro) > 0:
        # Converter nomes de unidades para ID_Sults
        ids_sults = []
        for unidade_nome in unidades_filtro:
            unidade_lower = unidade_nome.lower()
            # Buscar ID_Belle pelo nome
            for id_belle, info in MAPEAMENTO_IDS.items():
                if info['nome'].lower() == unidade_lower or info['nome'].lower().replace('buddha spa - ', '') == unidade_lower.replace('buddha spa - ', ''):
                    if info['id_sults']:
                        ids_sults.append(info['id_sults'])
                    break
        
        if ids_sults:
            ids_str = ','.join([str(id_s) for id_s in ids_sults])
            filtro_unidade = f"WHERE id IN ({ids_str})"
    
    query = f"""
    SELECT 
        id,
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
            return sorted(list(UNIDADE_BELLE_MAP.keys()))
        return df['unidade'].tolist()
    except Exception as e:
        st.error(f"Erro ao carregar unidades: {e}")
        return sorted(list(UNIDADE_BELLE_MAP.keys()))

@st.cache_data(ttl=3600)
def load_ecommerce_data(data_inicio, data_fim, unidades_filtro=None):
    """
    Carrega dados de ecommerce usando ID_Site (AFILLIATION_ID)
    """
    client = get_bigquery_client()
    
    filtro_unidade = ""
    if unidades_filtro and len(unidades_filtro) > 0:
        # Converter nomes de unidades para ID_Site
        ids_site = []
        for unidade_nome in unidades_filtro:
            unidade_lower = unidade_nome.lower()
            for id_belle, info in MAPEAMENTO_IDS.items():
                if info['nome'].lower() == unidade_lower or info['nome'].lower().replace('buddha spa - ', '') == unidade_lower.replace('buddha spa - ', ''):
                    ids_site.append(info['id_site'])
                    break
        
        if ids_site:
            ids_str = ','.join([str(id_s) for id_s in ids_site])
            filtro_unidade = f"AND CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) IN ({ids_str})"
    
    query = f"""
    SELECT 
        s.ID, s.NAME, s.STATUS, s.COUPONS, 
        s.CREATED_DATE, DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
        s.USED_DATE, DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL, 
        s.PRICE_NET, s.PRICE_GROSS, s.PRICE_REFOUND, s.KEY, s.ORDER_ID,
        (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
        CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) AS AFILLIATION_ID,
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
    LEFT JOIN `buddha-bigdata.raw.wp_posts` u 
        ON u.post_type = 'unidade' 
        AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
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
    """
    Carrega vouchers omnichannel usando ID_Site
    """
    client = get_bigquery_client()
    
    filtro_unidade = ""
    if unidades_filtro and len(unidades_filtro) > 0:
        ids_site = []
        for unidade_nome in unidades_filtro:
            unidade_lower = unidade_nome.lower()
            for id_belle, info in MAPEAMENTO_IDS.items():
                if info['nome'].lower() == unidade_lower or info['nome'].lower().replace('buddha spa - ', '') == unidade_lower.replace('buddha spa - ', ''):
                    ids_site.append(info['id_site'])
                    break
        
        if ids_site:
            ids_str = ','.join([str(id_s) for id_s in ids_site])
            filtro_unidade = f"AND CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) IN ({ids_str})"
    
    query = f"""
    SELECT 
        s.ID, s.NAME, s.STATUS, s.COUPONS, 
        s.CREATED_DATE, DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
        s.USED_DATE, DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL,
        s.PRICE_NET, s.PRICE_GROSS, s.PRICE_REFOUND, s.KEY, s.ORDER_ID,
        (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
        CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) AS AFILLIATION_ID,
        u.post_title AS AFILLIATION_NAME,
        (SELECT CONCAT(
            MAX(CASE WHEN pm.meta_key = '_billing_first_name' THEN pm.meta_value END), ' ',
            MAX(CASE WHEN pm.meta_key = '_billing_last_name' THEN pm.meta_value END))
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_FullName,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_email' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_Email,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_phone' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_Phone,
        (SELECT MAX(CASE WHEN usermeta.meta_key = 'billing_cpf' THEN usermeta.meta_value END)
         FROM `buddha-bigdata.raw.wp_postmeta` pm 
         LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta 
            ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
         WHERE pm.meta_key = '_customer_user' 
            AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_CPF,
        (SELECT MAX(CASE WHEN usermeta.meta_key = 'billing_cnpj' THEN usermeta.meta_value END)
         FROM `buddha-bigdata.raw.wp_postmeta` pm 
         LEFT JOIN `buddha-bigdata.raw.usermeta_raw` usermeta 
            ON CAST(CAST(pm.meta_value AS FLOAT64) AS INT64) = usermeta.user_id
         WHERE pm.meta_key = '_customer_user' 
            AND pm.post_id = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_CNPJ,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_City,
        (SELECT MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END)
         FROM `buddha-bigdata.raw.wp_posts` o 
         LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
         WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_State
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    INNER JOIN `buddha-bigdata.raw.wp_posts` u 
        ON u.post_type = 'unidade' 
        AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
        AND s.NAME LIKE CONCAT('% - ', u.post_title, '%')
    WHERE s.CREATED_DATE >= TIMESTAMP('{data_inicio} 00:00:00', 'America/Sao_Paulo')
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
data_inicio = col1.date_input("De:", value=datetime(2023, 1, 1), format="DD/MM/YYYY")
data_fim = col2.date_input("Até:", value=datetime(2023, 1, 31), format="DD/MM/YYYY")

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
    
    with st.spinner("Carregando informações das unidades..."):
        df_info_unidades = load_info_unidades(unidades_selecionadas if is_admin else [unidade_usuario])
    
    if not df_info_unidades.empty:
        df_info_display = df_info_unidades.copy()
        df_info_display['data_inauguracao'] = df_info_display['data_inauguracao'].apply(formatar_data)
        df_info_display['quantidade_macas'] = df_info_display['quantidade_macas'].fillna(0).astype(int)
        df_info_display['banho'] = df_info_display['banho'].fillna(0).astype(int)
        df_info_display['ayurvedica'] = df_info_display['ayurvedica'].fillna(0).astype(int)
        
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
        
        colunas_exibir = ['Unidade', 'Coordenador Comercial', 'Nº Macas', 'Cluster', 'Salas Banho', 'Salas Ayurvédica', 
                         'Data Inauguração', 'Cidade', 'UF', 'Status', 'Tipo']
        df_info_display = df_info_display[colunas_exibir]
        st.dataframe(df_info_display, use_container_width=True, height=400)
        
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
        
        st.markdown("---")
        col_g1, col_g2 = st.columns(2)
        
        with col_g1:
            st.markdown("#### Distribuição por Cluster")
            df_cluster = df_info_unidades.groupby('cluster').size().reset_index(name='quantidade')
            df_cluster = df_cluster.sort_values('quantidade', ascending=False)
            fig_cluster = px.bar(df_cluster, x='quantidade', y='cluster', orientation='h', text='quantidade')
            fig_cluster.update_yaxes(autorange='reversed')
            fig_cluster.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
            fig_cluster.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=350)
            st.plotly_chart(fig_cluster, use_container_width=True)
        
        with col_g2:
            st.markdown("#### Distribuição por Coordenador")
            df_coord = df_info_unidades.groupby('coordenador_comercial').size().reset_index(name='quantidade')
            df_coord = df_coord.sort_values('quantidade', ascending=False).head(10)
            fig_coord = px.bar(df_coord, x='quantidade', y='coordenador_comercial', orientation='h', text='quantidade')
            fig_coord.update_yaxes(autorange='reversed')
            fig_coord.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
            fig_coord.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=350)
            st.plotly_chart(fig_coord, use_container_width=True)
    else:
        st.warning("Nenhuma informação de unidade encontrada.")
    
    st.markdown("---")
    st.subheader("📈 Evolução da Receita")
    
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
    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.subheader("💰 Receita por Unidade")
    df_unidades = df.groupby('unidade')[valor_col].sum().reset_index().sort_values(valor_col, ascending=False)
    df_unidades['receita_fmt'] = df_unidades[valor_col].apply(formatar_moeda)
    fig_u = px.bar(df_unidades, x=valor_col, y='unidade', orientation='h', text='receita_fmt', labels={valor_col: 'Receita (R$)', 'unidade': 'Unidade'})
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
                'servico': 'Serviço', 'receita': 'Receita', 'qtd': 'Quantidade', 'perc_receita': '% Receita'
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
        'Origem': ['Belle\n(Sistema Local)', 'Ecommerce\n(Vouchers)', 'Parcerias\n(Cupons)'],
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
    
    ### 🔑 Mapeamento de IDs
    
    **ID_Belle** – ID usado no sistema Belle (atendimentos presenciais)
    
    **ID_Site** – ID usado no WordPress/Ecommerce (AFILLIATION_ID)
    
    **ID_Sults** – ID usado no sistema Sults (informações das unidades)
    
    O dashboard agora faz os JOINs corretos usando esses IDs para garantir precisão nos dados!
    
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
    - ID da unidade: `AFILLIATION_ID` (corresponde ao ID_Site)
    - Usado para: Vouchers vendidos online
    
    **3. Tabela de Unidades (View)**
    - Nome: `buddha-bigdata.analytics.unidades_view`
    - ID da unidade: `id` (corresponde ao ID_Sults)
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
