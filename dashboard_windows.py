"""
═══════════════════════════════════════════════════════════════════════════════
BUDDHA SPA - DASHBOARD DE FRANQUEADOS
Portal Analítico para Gestão de Unidades
═══════════════════════════════════════════════════════════════════════════════

VERSÃO: 3.1 - Completo, Documentado e Atualizado
DATA: Janeiro 2026

═══════════════════════════════════════════════════════════════════════════════
📊 COMO FUNCIONA O CÁLCULO DA RECEITA TOTAL
═══════════════════════════════════════════════════════════════════════════════

A Receita Total mostrada no dashboard vem de 3 ORIGENS diferentes:

1. 🏪 BELLE (Sistema Local)
   - Atendimentos pagos DIRETAMENTE na unidade
   - Cliente chega, faz o serviço, paga no local (cartão/dinheiro/PIX)
   - Registrado no sistema Belle (sistema de gestão local da unidade)
   - Exemplo: R$ 139.660,00

2. 🛒 ECOMMERCE (Vouchers SEM Cupom)
   - Cliente compra voucher no SITE Buddha Spa
   - NÃO usa cupom de desconto
   - Vai na unidade e USA o voucher
   - Terapeuta atende o cliente normalmente
   - Receita conta para a unidade onde foi USADO
   - Exemplo: R$ 81.332,04

3. 🤝 PARCERIAS (Vouchers COM Cupom)
   - Cliente compra voucher no SITE Buddha Spa
   - USA cupom de desconto de parceiro/empresa
   - Vai na unidade e USA o voucher
   - Receita conta para a unidade onde foi USADO
   - Exemplo: R$ 27.930,97

RECEITA TOTAL = Belle + Ecommerce + Parcerias
RECEITA TOTAL = R$ 139.660,00 + R$ 81.332,04 + R$ 27.930,97
RECEITA TOTAL = R$ 248.923,01 ✅

═══════════════════════════════════════════════════════════════════════════════
🔍 POR QUE OS VALORES PODEM PARECER DIFERENTES?
═══════════════════════════════════════════════════════════════════════════════

Você pode ver valores diferentes em diferentes partes do dashboard:

1. "PRINCIPAIS SERVIÇOS" mostra R$ 112.994,00 e 1.141 atendimentos
   POR QUÊ É DIFERENTE?
   - O gráfico mostra apenas os TOP 15 serviços mais vendidos
   - Não mostra TODOS os serviços
   - É um recorte para facilitar a visualização
   - Se somar TODOS os serviços, daria R$ 139.660,00 (Belle completo)

2. "ECOMMERCE" mostra valores diferentes em lugares diferentes
   POR QUÊ?
   - Aba "Visão Geral": mostra parte do ecommerce (R$ 81.332,04)
   - Aba "Marketing": mostra TODOS os vouchers utilizados (R$ 109.263,01)
   - A diferença vem de vouchers com cupons de parceiros

═══════════════════════════════════════════════════════════════════════════════
📅 ENTENDENDO AS DATAS
═══════════════════════════════════════════════════════════════════════════════

VOUCHERS TÊM 2 DATAS IMPORTANTES:

1. CREATED_DATE (Data de Compra)
   - Quando o cliente COMPROU o voucher no site
   - Usado na aba "Vouchers Omnichannel"
   - Mostra: "Quantos vouchers foram VENDIDOS para minha unidade?"

2. USED_DATE (Data de Uso)
   - Quando o cliente FOI NA UNIDADE e USOU o voucher
   - Usado na aba "Ecommerce - Vouchers Utilizados"
   - Mostra: "Quantos vouchers foram ATENDIDOS na minha unidade?"

EXEMPLO:
- Cliente compra voucher em 15/janeiro (CREATED_DATE)
- Cliente usa na unidade em 20/janeiro (USED_DATE)
- No dashboard aparece em 20/janeiro (quando gerou o atendimento)

═══════════════════════════════════════════════════════════════════════════════
🎯 MÉTRICAS PRINCIPAIS
═══════════════════════════════════════════════════════════════════════════════

1. RECEITA TOTAL (R$ 248.923,01)
   - Soma de TODAS as origens: Belle + Ecommerce + Parcerias
   - Todo o dinheiro que entrou na unidade no período

2. QUANTIDADE DE ATENDIMENTOS (1.304)
   - Quantos atendimentos únicos foram realizados
   - Conta cada ID de venda = 1 atendimento
   - Inclui APENAS Belle (presenciais diretos)

3. CLIENTES ÚNICOS (1.093)
   - Quantos clientes DIFERENTES foram atendidos
   - João fez 3 massagens = 3 atendimentos, mas 1 cliente único

4. TICKET MÉDIO (R$ 227,83)
   - Receita Belle ÷ Atendimentos Pagos
   - Usa APENAS Belle porque vouchers já foram pagos antes
   - Mede: "Quanto o cliente gasta POR ATENDIMENTO na unidade?"

═══════════════════════════════════════════════════════════════════════════════
"""

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

def formatar_data(data):
    """Formata data para dd/mm/yyyy"""
    if pd.isna(data):
        return ""
    try:
        return pd.to_datetime(data).strftime('%d/%m/%Y')
    except:
        return str(data)

def adicionar_totalizador(df, colunas_numericas, primeira_coluna='', calcular_ticket_medio=False):
    """
    Adiciona linha de total ao dataframe

    Args:
        df: DataFrame original
        colunas_numericas: lista de colunas que devem ser somadas
        primeira_coluna: nome da primeira coluna (onde aparecerá 'TOTAL')
        calcular_ticket_medio: se True, recalcula ticket médio no total
    """
    if df.empty:
        return df

    total_row = {}
    for col in df.columns:
        if col in colunas_numericas:
            # Somar valores numéricos
            if df[col].dtype in ['int64', 'float64']:
                total_row[col] = df[col].sum()
            else:
                total_row[col] = ''
        elif col == 'ticket_medio' and calcular_ticket_medio:
            # Recalcular ticket médio se solicitado
            if 'receita' in df.columns and 'qtd_atendimentos' in df.columns:
                total_receita = df['receita'].sum()
                total_qtd = df['qtd_atendimentos'].sum()
                total_row[col] = total_receita / total_qtd if total_qtd > 0 else 0
            else:
                total_row[col] = ''
        else:
            # Primeira coluna recebe 'TOTAL'
            if col == (primeira_coluna or df.columns[0]):
                total_row[col] = 'TOTAL'
            else:
                total_row[col] = ''

    df_com_total = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
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
        /* Estilo para tabelas */
        .dataframe-custom {
            font-size: 0.9rem;
        }
        .dataframe-custom th {
            background-color: #8B0000;
            color: white;
        }
        .dataframe-custom td {
            text-align: left; /* Alinha texto à esquerda */
        }
        .dataframe-custom tr:nth-child(even) {
            background-color: #f9f9f9;
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
# NOVA FUNÇÃO: Carregar dados adicionais das unidades
# -----------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_dados_unidades():
    """Carrega dados adicionais das unidades da tabela unidades_view."""
    client = get_bigquery_client()
    query = """
    SELECT
        LOWER(nome_fantasia) AS nome_fantasia_lower,
        coordenador_comercial,
        quantidade_macas,
        cluster,
        banho AS salas_banho,
        ayurvedica AS salas_ayurvedica,
        data_inauguracao
    FROM `buddha-bigdata.analytics.unidades_view`
    """
    df_unidades_extra = client.query(query).to_dataframe()

    # Normalizar o nome da unidade para combinar com o Belle
    df_unidades_extra['unidade_normalizada'] = df_unidades_extra['nome_fantasia_lower'].str.lower().str.strip()
    df_unidades_extra.drop(columns=['nome_fantasia_lower'], inplace=True)

    return df_unidades_extra


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
# CARREGAR DADOS DE ECOMMERCE PARA CALCULAR RECEITA TOTAL
# -----------------------------------------------------------------------------
with st.spinner("Calculando faturamento total..."):
    try:
        unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
        df_ecom_fat = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
    except Exception as e:
        st.error(f"Erro ao carregar ecommerce: {e}")
        df_ecom_fat = pd.DataFrame()

# Calcular receitas por origem
receita_belle = df[valor_col].sum()  # Receita dos atendimentos presenciais
receita_ecommerce = 0
receita_parceiro = 0

if not df_ecom_fat.empty:
    df_ecom_fat['PRICE_NET'] = pd.to_numeric(df_ecom_fat['PRICE_NET'], errors='coerce')

    # Separar vouchers com e sem cupom
    df_ecom_sem_cupom = df_ecom_fat[df_ecom_fat['COUPONS'].isna() | (df_ecom_fat['COUPONS'] == '')]
    df_ecom_com_cupom = df_ecom_fat[df_ecom_fat['COUPONS'].notna() & (df_ecom_fat['COUPONS'] != '')]

    receita_ecommerce = df_ecom_sem_cupom['PRICE_NET'].fillna(0).sum()
    receita_parceiro = df_ecom_com_cupom['PRICE_NET'].fillna(0).sum()

# RECEITA TOTAL = Belle + Ecommerce + Parcerias
receita_total = receita_belle + receita_ecommerce + receita_parceiro

# -----------------------------------------------------------------------------
# HEADER / KPIs
# -----------------------------------------------------------------------------
col_logo, col_title = st.columns([1, 5])

with col_logo:
    st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTkFQZ7QiSmOpEWC_9Ndsuqx_-roUMRJJkCvw&s", width=200)

with col_title:
    st.title("Buddha Spa - Dashboard de Unidades")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0

# Calcular ticket médio apenas com atendimentos que geraram receita
df_com_receita = df[df[valor_col] > 0]
qtd_atendimentos_pagos = int(df_com_receita['id_venda'].nunique())
ticket_medio = receita_belle / qtd_atendimentos_pagos if qtd_atendimentos_pagos > 0 else 0

# KPIs PRINCIPAIS COM AJUDA
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

# Mostrar unidades selecionadas
if is_admin and unidades_selecionadas:
    st.markdown("---")
    st.info(f"**📍 Unidades selecionadas:** {', '.join([u.title() for u in unidades_selecionadas])}")
elif not is_admin:
    st.markdown("---")
    st.info(f"**📍 Visualizando unidade:** {unidade_usuario.title()}")

# Expandir explicação da Receita Total
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

    #### 📍 O que está incluído:

    **Atendimentos Presenciais Pagos (Belle)**
    - Todos os serviços realizados e pagos na unidade
    - Apenas o **valor líquido** (já descontado impostos e taxas)

    **Vouchers Utilizados (Ecommerce + Parcerias)**
    - Vouchers comprados no site e utilizados na sua unidade
    - Baseado na data de utilização (USED_DATE)

    #### 🔍 Detalhamento:

    - **Período**: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
    - **Total de atendimentos**: {formatar_numero(qtd_atendimentos)}
    - **Clientes únicos**: {formatar_numero(qtd_clientes)}
    - **Ticket médio (Belle)**: {formatar_moeda(ticket_medio)}

    #### ❌ NÃO incluído:
    - Produtos vendidos (cosméticos, óleos, etc.)
    - Vouchers vendidos mas ainda não utilizados
    - Vendas canceladas ou reembolsadas

    #### 💡 Quer ver mais detalhes?

    - **Aba Financeiro**: Veja a distribuição completa por origem (Belle, Ecommerce, Parcerias)
    - **Aba Atendimento**: Veja quais serviços geraram mais receita
    - **Aba Marketing & Ecommerce**: Veja os vouchers utilizados
    """)

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

    # NOVA SEÇÃO: Informações das Unidades
    st.subheader("Informações das Unidades")

    # Carregar dados extras das unidades
    with st.spinner("Carregando informações adicionais das unidades..."):
        try:
            df_unidades_extra = load_dados_unidades()
        except Exception as e:
            st.error(f"Erro ao carregar dados adicionais das unidades: {e}")
            df_unidades_extra = pd.DataFrame()

    # Filtrar e preparar dados para exibição
    if not df_unidades_extra.empty:
        # Filtrar unidades com base na seleção do usuário
        if is_admin:
            if unidades_selecionadas:
                # Normalizar nomes para comparação
                unidades_selecionadas_norm = [u.strip().lower() for u in unidades_selecionadas]
                df_unidades_filtradas = df_unidades_extra[
                    df_unidades_extra['unidade_normalizada'].isin(unidades_selecionadas_norm)
                ].copy()
            else:
                df_unidades_filtradas = df_unidades_extra.copy()
        else:
            unidade_usuario_norm = unidade_usuario.strip().lower()
            df_unidades_filtradas = df_unidades_extra[
                df_unidades_extra['unidade_normalizada'] == unidade_usuario_norm
            ].copy()

        if not df_unidades_filtradas.empty:
            # Selecionar e renomear colunas para exibição
            colunas_exibir = [
                'unidade_normalizada',
                'coordenador_comercial',
                'quantidade_macas',
                'cluster',
                'salas_banho',
                'salas_ayurvedica',
                'data_inauguracao'
            ]

            df_unidades_display = df_unidades_filtradas[colunas_exibir].copy()

            # Renomear colunas para melhor apresentação
            df_unidades_display.rename(columns={
                'unidade_normalizada': 'Unidade',
                'coordenador_comercial': 'Coordenador Comercial',
                'quantidade_macas': 'Nº de Macas',
                'cluster': 'Cluster',
                'salas_banho': 'Salas Banho',
                'salas_ayurvedica': 'Salas Ayurvédicas',
                'data_inauguracao': 'Data Inauguração'
            }, inplace=True)

            # Formatar a data de inauguração
            df_unidades_display['Data Inauguração'] = df_unidades_display['Data Inauguração'].apply(formatar_data)

            # Resetar índice para começar do 0
            df_unidades_display.reset_index(drop=True, inplace=True)

            # Exibir a tabela com estilo personalizado
            st.dataframe(
                df_unidades_display,
                use_container_width=True,
                height=400,
                hide_index=True
            )
        else:
            st.info("Nenhuma informação adicional encontrada para as unidades selecionadas.")
    else:
        st.warning("Não foi possível carregar as informações adicionais das unidades.")

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

        # Adicionar totalizador ANTES de formatar (com recalculo de ticket médio)
        df_terap_com_total = adicionar_totalizador(
            df_terap,
            colunas_numericas=['receita', 'qtd_atendimentos', 'clientes_unicos'],
            primeira_coluna='unidade',
            calcular_ticket_medio=True
        )

        # Agora formatar
        df_terap_display = df_terap_com_total.copy()
        df_terap_display['receita'] = df_terap_display['receita'].apply(
            lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
        )
        df_terap_display['qtd_atendimentos'] = df_terap_display['qtd_atendimentos'].apply(
            lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
        )
        df_terap_display['clientes_unicos'] = df_terap_display['clientes_unicos'].apply(
            lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
        )
        df_terap_display['ticket_medio'] = df_terap_display['ticket_medio'].apply(
            lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
        )

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
            # Adicionar totalizador
            df_servicos_com_total = adicionar_totalizador(
                df_servicos,
                colunas_numericas=['receita', 'qtd'],
                primeira_coluna='nome_servico_simplificado'
            )

            # Formatar tabela
            df_servicos_display = df_servicos_com_total.copy()
            df_servicos_display['receita_fmt'] = df_servicos_display['receita'].apply(
                lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
            )
            df_servicos_display['qtd_fmt'] = df_servicos_display['qtd'].apply(
                lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
            )
            # Para percentual, só calcular para linhas não-total
            df_servicos_display['perc_receita_fmt'] = df_servicos_display.apply(
                lambda row: formatar_percentual(row['perc_receita']*100) if pd.notna(row.get('perc_receita')) and row.get('perc_receita') != '' else '100,00%' if row['nome_servico_simplificado'] == 'TOTAL' else '',
                axis=1
            )

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

    # GRÁFICO DE BARRAS: Atendimentos por Dia da Semana vs Unidade
    col_titulo_bar1, col_ajuda_bar1 = st.columns([0.97, 0.03])
    with col_titulo_bar1:
        st.subheader("Atendimentos por Dia da Semana e Unidade")
    with col_ajuda_bar1:
        with st.popover("ℹ️"):
            st.caption("Quantidade de atendimentos por dia da semana em cada unidade. Barras mais altas indicam maior volume.")

    # Adicionar dia da semana ao dataframe
    df_heatmap = df_detalhado.copy()
    df_heatmap['dia_semana'] = pd.to_datetime(df_heatmap[data_col]).dt.day_name()

    # Traduzir dias da semana para português
    dias_semana_map = {
        'Monday': 'Segunda',
        'Tuesday': 'Terça',
        'Wednesday': 'Quarta',
        'Thursday': 'Quinta',
        'Friday': 'Sexta',
        'Saturday': 'Sábado',
        'Sunday': 'Domingo'
    }
    df_heatmap['dia_semana'] = df_heatmap['dia_semana'].map(dias_semana_map)

    # Agrupar por dia da semana e unidade
    df_bar_unidade = (
        df_heatmap.groupby(['dia_semana', 'unidade'])
        .size()
        .reset_index(name='qtd_atendimentos')
    )

    # Ordenar dias da semana
    dias_ordem = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']

    # Criar gráfico de barras agrupadas
    fig_bar1 = px.bar(
        df_bar_unidade,
        x='dia_semana',
        y='qtd_atendimentos',
        color='unidade',
        barmode='group',
        labels={'dia_semana': 'Dia da Semana', 'qtd_atendimentos': 'Atendimentos', 'unidade': 'Unidade'},
        category_orders={'dia_semana': dias_ordem},
        text='qtd_atendimentos'
    )

    fig_bar1.update_traces(textposition='outside', textfont=dict(size=10))
    fig_bar1.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        height=450,
        xaxis_title="Dia da Semana",
        yaxis_title="Quantidade de Atendimentos",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig_bar1, use_container_width=True, key="chart_bar_semana_unidade")

    st.markdown("---")

    # GRÁFICO DE BARRAS: Atendimentos por Dia da Semana vs Tipo de Serviço
    col_titulo_bar2, col_ajuda_bar2 = st.columns([0.97, 0.03])
    with col_titulo_bar2:
        st.subheader("Atendimentos por Dia da Semana e Tipo de Serviço")
    with col_ajuda_bar2:
        with st.popover("ℹ️"):
            st.caption("Top 10 serviços mais populares e em quais dias da semana têm maior demanda.")

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
        df_bar_servico = (
            df_heatmap[df_heatmap['nome_servico_simplificado'].isin(top_servicos)]
            .groupby(['dia_semana', 'nome_servico_simplificado'])
            .size()
            .reset_index(name='qtd_atendimentos')
        )

        # Criar gráfico de barras agrupadas
        fig_bar2 = px.bar(
            df_bar_servico,
            x='dia_semana',
            y='qtd_atendimentos',
            color='nome_servico_simplificado',
            barmode='group',
            labels={'dia_semana': 'Dia da Semana', 'qtd_atendimentos': 'Atendimentos', 'nome_servico_simplificado': 'Serviço'},
            category_orders={'dia_semana': dias_ordem},
            text='qtd_atendimentos'  # ADICIONADO: Rótulos nas barras
        )

        # ADICIONADO: Configurar posição e tamanho dos rótulos
        fig_bar2.update_traces(textposition='outside', textfont=dict(size=9))

        fig_bar2.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=450,
            xaxis_title="Dia da Semana",
            yaxis_title="Quantidade de Atendimentos",
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02, title="Serviço")
        )

        st.plotly_chart(fig_bar2, use_container_width=True, key="chart_bar_semana_servico")

# ---------------------- TAB: FINANCEIRO -------------------------
with tab_fin:
    st.subheader("Resumo Financeiro da Unidade")

    colf1, colf2, colf3 = st.columns(3)

    with colf1:
        st.metric("Receita Total (Atendimentos)", formatar_moeda(receita_belle))
        with st.popover("ℹ️"):
            st.caption("Receita total dos atendimentos presenciais (Belle - não inclui vouchers de ecommerce).")

    with colf2:
        st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
        with st.popover("ℹ️"):
            st.caption("Total de atendimentos únicos realizados.")

    with colf3:
        st.metric("Ticket Médio por Atendimento", formatar_moeda(ticket_medio))
        with st.popover("ℹ️"):
            st.caption("Valor médio gasto por atendimento (Belle). Calculado como: Receita Belle ÷ Quantidade de Atendimentos que geraram receita.")

    st.markdown("---")

    # Distribuição da Receita por Origem
    st.subheader("Distribuição da Receita por Origem")

    df_origem = pd.DataFrame({
        'Origem': ['Belle (Presencial)', 'Ecommerce (Vouchers)', 'Parcerias (Cupons)'],
        'Valor': [receita_belle, receita_ecommerce, receita_parceiro],
        'Porcentagem': [receita_belle/receita_total*100, receita_ecommerce/receita_total*100, receita_parceiro/receita_total*100]
    })
    df_origem['Valor_Formatado'] = df_origem['Valor'].apply(formatar_moeda)
    df_origem['Porcentagem_Formatada'] = df_origem['Porcentagem'].apply(formatar_percentual)

    fig_origem = px.pie(
        df_origem,
        names='Origem',
        values='Valor',
        color='Origem',
        color_discrete_map={
            'Belle (Presencial)': '#8B0000',
            'Ecommerce (Vouchers)': '#2E7D32',
            'Parcerias (Cupons)': '#1565C0'
        },
        hover_data=['Valor_Formatado', 'Porcentagem_Formatada']
    )
    fig_origem.update_traces(textposition='inside', textinfo='percent+label')
    fig_origem.update_layout(paper_bgcolor='#F5F0E6', height=400)
    st.plotly_chart(fig_origem, use_container_width=True, key="chart_distribuicao_origem")

    st.markdown("---")

    # Receita por Forma de Pagamento
    st.subheader("Receita por Forma de Pagamento")

    if 'forma_pagamento' in df.columns:
        df_pag = (
            df.groupby('forma_pagamento')[valor_col]
            .sum()
            .reset_index()
            .sort_values(valor_col, ascending=False)
        )
        df_pag['receita_fmt'] = df_pag[valor_col].apply(formatar_moeda)
        df_pag['perc'] = df_pag[valor_col] / df_pag[valor_col].sum() * 100
        df_pag['perc_fmt'] = df_pag['perc'].apply(formatar_percentual)

        fig_pag = px.bar(
            df_pag,
            x=valor_col,
            y='forma_pagamento',
            orientation='h',
            text='perc_fmt',
            labels={valor_col: 'Receita (R$)', 'forma_pagamento': 'Forma de Pagamento'}
        )
        fig_pag.update_yaxes(autorange='reversed')
        fig_pag.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
        fig_pag.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400,
            yaxis={'categoryorder': 'total descending'}
        )
        fig_pag.update_xaxes(tickformat=",.2f")
        st.plotly_chart(fig_pag, use_container_width=True, key="chart_receita_forma_pagamento")

        # Tabela de formas de pagamento
        st.dataframe(
            df_pag[['forma_pagamento', 'receita_fmt', 'perc_fmt']].rename(columns={
                'forma_pagamento': 'Forma de Pagamento',
                'receita_fmt': 'Receita',
                'perc_fmt': '% Participação'
            }),
            use_container_width=True,
            hide_index=True
        )

    st.markdown("---")

    # Receita por Tipo de Serviço
    st.subheader("Receita por Tipo de Serviço")

    if 'nome_servico_simplificado' in df.columns:
        df_tipo = (
            df.groupby('nome_servico_simplificado')[valor_col]
            .sum()
            .reset_index()
            .sort_values(valor_col, ascending=False)
        )
        df_tipo['receita_fmt'] = df_tipo[valor_col].apply(formatar_moeda)
        df_tipo['perc'] = df_tipo[valor_col] / df_tipo[valor_col].sum() * 100
        df_tipo['perc_fmt'] = df_tipo['perc'].apply(formatar_percentual)

        fig_tipo = px.bar(
            df_tipo.head(15),
            x=valor_col,
            y='nome_servico_simplificado',
            orientation='h',
            text='perc_fmt',
            labels={valor_col: 'Receita (R$)', 'nome_servico_simplificado': 'Tipo de Serviço'}
        )
        fig_tipo.update_yaxes(autorange='reversed')
        fig_tipo.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
        fig_tipo.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=500,
            yaxis={'categoryorder': 'total descending'}
        )
        fig_tipo.update_xaxes(tickformat=",.2f")
        st.plotly_chart(fig_tipo, use_container_width=True, key="chart_receita_tipo_servico")

        # Tabela de tipos de serviço
        st.dataframe(
            df_tipo[['nome_servico_simplificado', 'receita_fmt', 'perc_fmt']].rename(columns={
                'nome_servico_simplificado': 'Tipo de Serviço',
                'receita_fmt': 'Receita',
                'perc_fmt': '% Participação'
            }),
            use_container_width=True,
            hide_index=True
        )

# ---------------------- TAB: MARKETING & ECOMMERCE -------------------------
with tab_mkt:
    st.subheader("Vouchers Utilizados (Ecommerce)")

    # Carregar dados de ecommerce
    with st.spinner("Carregando dados de vouchers..."):
        try:
            unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
            df_ecom = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar dados de ecommerce: {e}")
            df_ecom = pd.DataFrame()

    if not df_ecom.empty:
        # Converter colunas numéricas
        df_ecom['PRICE_NET'] = pd.to_numeric(df_ecom['PRICE_NET'], errors='coerce')
        df_ecom['PRICE_GROSS'] = pd.to_numeric(df_ecom['PRICE_GROSS'], errors='coerce')

        # Separar vouchers com e sem cupom
        df_ecom_sem_cupom = df_ecom[df_ecom['COUPONS'].isna() | (df_ecom['COUPONS'] == '')]
        df_ecom_com_cupom = df_ecom[df_ecom['COUPONS'].notna() & (df_ecom['COUPONS'] != '')]

        # Métricas principais
        colm1, colm2, colm3, colm4 = st.columns(4)
        with colm1:
            st.metric("Total de Vouchers Utilizados", formatar_numero(len(df_ecom)))
            with st.popover("ℹ️"):
                st.caption("Quantidade total de vouchers utilizados no período.")

        with colm2:
            st.metric("Receita Total de Vouchers", formatar_moeda(df_ecom['PRICE_NET'].sum()))
            with st.popover("ℹ️"):
                st.caption("Valor líquido total dos vouchers utilizados.")

        with colm3:
            st.metric("Vouchers SEM Cupom", formatar_numero(len(df_ecom_sem_cupom)))
            with st.popover("ℹ️"):
                st.caption("Vouchers comprados diretamente no site (sem cupom).")

        with colm4:
            st.metric("Vouchers COM Cupom", formatar_numero(len(df_ecom_com_cupom)))
            with st.popover("ℹ️"):
                st.caption("Vouchers comprados com cupom de parceiro.")

        st.markdown("---")

        # Evolução diária de vouchers utilizados
        st.subheader("Evolução Diária de Vouchers Utilizados")

        df_ecom['USED_DATE_BRAZIL'] = pd.to_datetime(df_ecom['USED_DATE_BRAZIL'])
        df_ecom['data_utilizacao'] = df_ecom['USED_DATE_BRAZIL'].dt.date

        df_evolucao_vouchers = (
            df_ecom.groupby('data_utilizacao')
            .agg({'PRICE_NET': 'sum', 'ID': 'count'})
            .reset_index()
            .rename(columns={'ID': 'qtd_vouchers'})
        )

        fig_ev_vouchers = px.line(
            df_evolucao_vouchers,
            x='data_utilizacao',
            y=['qtd_vouchers', 'PRICE_NET'],
            labels={'data_utilizacao': 'Data', 'value': 'Valor', 'variable': 'Métrica'},
            markers=True
        )
        fig_ev_vouchers.update_layout(
            xaxis_title="Data",
            yaxis_title="Quantidade / Valor (R$)",
            height=400,
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            legend=dict(title="")
        )
        fig_ev_vouchers.for_each_trace(
            lambda trace: trace.update(name='Quantidade de Vouchers') if trace.name == 'qtd_vouchers' 
                          else trace.update(name='Receita (R$)', yaxis='y2')
        )
        st.plotly_chart(fig_ev_vouchers, use_container_width=True, key="chart_evolucao_vouchers")

        st.markdown("---")

        # Top Pacotes Vendidos
        st.subheader("Top Pacotes Vendidos")

        if 'PACKAGE_NAME' in df_ecom.columns:
            df_pacotes = (
                df_ecom.groupby('PACKAGE_NAME')['PRICE_NET']
                .agg(['sum', 'count'])
                .reset_index()
                .rename(columns={'sum': 'receita', 'count': 'qtd'})
                .sort_values('receita', ascending=False)
                .head(10)
            )
            df_pacotes['receita_fmt'] = df_pacotes['receita'].apply(formatar_moeda)
            df_pacotes['qtd_fmt'] = df_pacotes['qtd'].apply(formatar_numero)

            fig_pacotes = px.bar(
                df_pacotes,
                x='receita',
                y='PACKAGE_NAME',
                orientation='h',
                text='qtd_fmt',
                labels={'receita': 'Receita (R$)', 'PACKAGE_NAME': 'Pacote'}
            )
            fig_pacotes.update_yaxes(autorange='reversed')
            fig_pacotes.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white'))
            fig_pacotes.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=400,
                yaxis={'categoryorder': 'total descending'}
            )
            fig_pacotes.update_xaxes(tickformat=",.2f")
            st.plotly_chart(fig_pacotes, use_container_width=True, key="chart_top_pacotes")

            # Tabela de pacotes
            st.dataframe(
                df_pacotes[['PACKAGE_NAME', 'receita_fmt', 'qtd_fmt']].rename(columns={
                    'PACKAGE_NAME': 'Pacote',
                    'receita_fmt': 'Receita',
                    'qtd_fmt': 'Quantidade'
                }),
                use_container_width=True,
                hide_index=True
            )

        st.markdown("---")

        # Vouchers Omnichannel (Compras)
        st.subheader("Vouchers Omnichannel (Compras)")

        with st.spinner("Carregando dados omnichannel..."):
            try:
                df_omni = load_omnichannel_vouchers(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
            except Exception as e:
                st.error(f"Erro ao carregar dados omnichannel: {e}")
                df_omni = pd.DataFrame()

        if not df_omni.empty:
            # Converter colunas numéricas
            df_omni['PRICE_NET'] = pd.to_numeric(df_omni['PRICE_NET'], errors='coerce')

            # Métricas omnichannel
            colo1, colo2, colo3 = st.columns(3)
            with colo1:
                st.metric("Total de Vouchers Comprados", formatar_numero(len(df_omni)))
                with st.popover("ℹ️"):
                    st.caption("Quantidade total de vouchers comprados no período.")

            with colo2:
                st.metric("Receita Bruta de Vouchers", formatar_moeda(df_omni['PRICE_GROSS'].sum()))
                with st.popover("ℹ️"):
                    st.caption("Valor bruto total dos vouchers comprados.")

            with colo3:
                st.metric("Receita Líquida de Vouchers", formatar_moeda(df_omni['PRICE_NET'].sum()))
                with st.popover("ℹ️"):
                    st.caption("Valor líquido total dos vouchers comprados.")

            st.markdown("---")

            # Evolução diária de vouchers comprados
            st.subheader("Evolução Diária de Vouchers Comprados")

            df_omni['CREATED_DATE_BRAZIL'] = pd.to_datetime(df_omni['CREATED_DATE_BRAZIL'])
            df_omni['data_compra'] = df_omni['CREATED_DATE_BRAZIL'].dt.date

            df_evolucao_compras = (
                df_omni.groupby('data_compra')
                .agg({'PRICE_NET': 'sum', 'ID': 'count'})
                .reset_index()
                .rename(columns={'ID': 'qtd_vouchers'})
            )

            fig_ev_compras = px.line(
                df_evolucao_compras,
                x='data_compra',
                y=['qtd_vouchers', 'PRICE_NET'],
                labels={'data_compra': 'Data', 'value': 'Valor', 'variable': 'Métrica'},
                markers=True
            )
            fig_ev_compras.update_layout(
                xaxis_title="Data",
                yaxis_title="Quantidade / Valor (R$)",
                height=400,
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                legend=dict(title="")
            )
            fig_ev_compras.for_each_trace(
                lambda trace: trace.update(name='Quantidade de Vouchers') if trace.name == 'qtd_vouchers' 
                              else trace.update(name='Receita (R$)', yaxis='y2')
            )
            st.plotly_chart(fig_ev_compras, use_container_width=True, key="chart_evolucao_compras")

            st.markdown("---")

            # Tabela de vouchers comprados
            st.subheader("Detalhamento de Vouchers Comprados")

            df_omni_display = df_omni.copy()
            df_omni_display['CREATED_DATE_BRAZIL'] = df_omni_display['CREATED_DATE_BRAZIL'].dt.strftime('%d/%m/%Y %H:%M')
            df_omni_display['USED_DATE_BRAZIL'] = df_omni_display['USED_DATE_BRAZIL'].dt.strftime('%d/%m/%Y %H:%M')
            df_omni_display['PRICE_NET_FMT'] = df_omni_display['PRICE_NET'].apply(formatar_moeda)
            df_omni_display['PRICE_GROSS_FMT'] = df_omni_display['PRICE_GROSS'].apply(formatar_moeda)

            st.dataframe(
                df_omni_display[[
                    'CREATED_DATE_BRAZIL', 'USED_DATE_BRAZIL', 'NAME', 'AFILLIATION_NAME',
                    'Customer_FullName', 'Customer_Email', 'Customer_Phone', 'Customer_CPF', 'Customer_CNPJ',
                    'Customer_City', 'Customer_State', 'PRICE_GROSS_FMT', 'PRICE_NET_FMT'
                ]].rename(columns={
                    'CREATED_DATE_BRAZIL': 'Data Compra',
                    'USED_DATE_BRAZIL': 'Data Uso',
                    'NAME': 'Nome Voucher',
                    'AFILLIATION_NAME': 'Unidade',
                    'Customer_FullName': 'Cliente Nome',
                    'Customer_Email': 'Cliente Email',
                    'Customer_Phone': 'Cliente Telefone',
                    'Customer_CPF': 'Cliente CPF',
                    'Customer_CNPJ': 'Cliente CNPJ',
                    'Customer_City': 'Cliente Cidade',
                    'Customer_State': 'Cliente Estado',
                    'PRICE_GROSS_FMT': 'Valor Bruto',
                    'PRICE_NET_FMT': 'Valor Líquido'
                }),
                use_container_width=True,
                height=600
            )
        else:
            st.info("Sem dados omnichannel para o período selecionado.")
    else:
        st.info("Sem dados de vouchers para o período selecionado.")

# ---------------------- TAB: SELF-SERVICE -------------------------
with tab_selfservice:
    st.subheader("Relatórios Personalizados")

    st.markdown("""
    Esta seção permite que você gere relatórios personalizados com base nos filtros aplicados.
    
    Você pode:
    - Exportar dados brutos para análise em Excel
    - Gerar relatórios específicos por período
    - Comparar desempenho entre unidades
    
    Em breve, novas funcionalidades serão adicionadas aqui.
    """)

    # Botão para exportar dados brutos
    if st.button("Exportar Dados Brutos (Excel)"):
        # Criar um buffer para armazenar o arquivo Excel
        from io import BytesIO
        output = BytesIO()
        
        # Criar um escritor de Excel
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            # Exportar dados principais
            df.to_excel(writer, sheet_name='Atendimentos', index=False)
            
            # Exportar dados de ecommerce se disponíveis
            if 'df_ecom' in locals() and not df_ecom.empty:
                df_ecom.to_excel(writer, sheet_name='Ecommerce', index=False)
                
            # Exportar dados de NPS se disponíveis
            if 'df_nps' in locals() and not df_nps.empty:
                df_nps.to_excel(writer, sheet_name='NPS', index=False)
                
        # Obter os dados do buffer
        output.seek(0)
        
        # Criar nome do arquivo
        nome_arquivo = f"relatorio_buddha_spa_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.xlsx"
        
        # Botão de download
        st.download_button(
            label="Baixar Relatório",
            data=output,
            file_name=nome_arquivo,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    st.markdown("---")

    st.subheader("Comparativo de Períodos")

    st.markdown("""
    Compare o desempenho entre dois períodos diferentes.
    
    Selecione os períodos abaixo para ver uma comparação lado a lado.
    """)

    # Filtros para comparação
    col_comp1, col_comp2 = st.columns(2)
    
    with col_comp1:
        st.markdown("**Período 1**")
        data_inicio_1 = st.date_input("De (Período 1):", value=datetime(2025, 1, 1), format="DD/MM/YYYY")
        data_fim_1 = st.date_input("Até (Período 1):", value=datetime(2025, 1, 15), format="DD/MM/YYYY")
        
    with col_comp2:
        st.markdown("**Período 2**")
        data_inicio_2 = st.date_input("De (Período 2):", value=datetime(2025, 1, 16), format="DD/MM/YYYY")
        data_fim_2 = st.date_input("Até (Período 2):", value=datetime(2025, 1, 31), format="DD/MM/YYYY")

    # Botão para gerar comparação
    if st.button("Gerar Comparativo"):
        with st.spinner("Gerando comparativo..."):
            try:
                # Carregar dados para ambos os períodos
                if is_admin and not unidades_selecionadas:
                    df_1 = load_atendimentos(data_inicio_1, data_fim_1, unidade_filtro=None)
                    df_2 = load_atendimentos(data_inicio_2, data_fim_2, unidade_filtro=None)
                elif is_admin and unidades_selecionadas:
                    df_1 = load_atendimentos(data_inicio_1, data_fim_1, unidade_filtro=None)
                    df_1 = df_1[df_1['unidade'].str.lower().isin(unidades_selecionadas)]
                    df_2 = load_atendimentos(data_inicio_2, data_fim_2, unidade_filtro=None)
                    df_2 = df_2[df_2['unidade'].str.lower().isin(unidades_selecionadas)]
                else:
                    df_1 = load_atendimentos(data_inicio_1, data_fim_1, unidade_filtro=unidade_usuario)
                    df_2 = load_atendimentos(data_inicio_2, data_fim_2, unidade_filtro=unidade_usuario)
                    
                # Calcular métricas para cada período
                receita_1 = df_1[valor_col].sum()
                qtd_atend_1 = int(df_1['id_venda'].nunique())
                clientes_1 = int(df_1['nome_cliente'].nunique()) if 'nome_cliente' in df_1.columns else 0
                
                receita_2 = df_2[valor_col].sum()
                qtd_atend_2 = int(df_2['id_venda'].nunique())
                clientes_2 = int(df_2['nome_cliente'].nunique()) if 'nome_cliente' in df_2.columns else 0
                
                # Calcular variações
                var_receita = ((receita_2 - receita_1) / receita_1 * 100) if receita_1 > 0 else 0
                var_atend = ((qtd_atend_2 - qtd_atend_1) / qtd_atend_1 * 100) if qtd_atend_1 > 0 else 0
                var_clientes = ((clientes_2 - clientes_1) / clientes_1  100)) if clientes_1 > 0 else 0
                
                # Mostrar resultados
                st.markdown("### Comparativo de Desempenho")
                
                col_comp_res1, col_comp_res2, col_comp_res3 = st.columns(3)
                
                with col_comp_res1:
                    st.metric(
                        f"Receita ({data_inicio_1.strftime('%d/%m')} - {data_fim_1.strftime('%d/%m')})",
                        formatar_moeda(receita_1)
                    )
                    st.metric(
                        f"Receita ({data_inicio_2.strftime('%d/%m')} - {data_fim_2.strftime('%d/%m')})",
                        formatar_moeda(receita_2),
                        delta=f"{formatar_percentual(var_receita)}" if var_receita != 0 else None
                    )
                
                with col_comp_res2:
                    st.metric(
                        f"Atendimentos ({data_inicio_1.strftime('%d/%m')} - {data_fim_1.strftime('%d/%m')})",
                        formatar_numero(qtd_atend_1)
                    )
                    st.metric(
                        f"Atendimentos ({data_inicio_2.strftime('%d/%m')} - {data_fim_2.strftime('%d/%m')})",
                        formatar_numero(qtd_atend_2),
                        delta=f"{formatar_percentual(var_atend)}" if var_atend != 0 else None
                    )
                
                with col_comp_res3:
                    st.metric(
                        f"Clientes Únicos ({data_inicio_1.strftime('%d/%m')} - {data_fim_1.strftime('%d/%m')})",
                        formatar_numero(clientes_1)
                    )
                    st.metric(
                        f"Clientes Únicos ({data_inicio_2.strftime('%d/%m')} - {data_fim_2.strftime('%d/%m')})",
                        formatar_numero(clientes_2),
                        delta=f"{formatar_percentual(var_clientes)}" if var_clientes != 0 else None
                    )
                    
            except Exception as e:
                st.error(f"Erro ao gerar comparativo: {e}")

# ---------------------- TAB: AJUDA / GLOSSÁRIO -------------------------
with tab_gloss:
    st.subheader("Glossário de Termos")
    
    st.markdown("""
    ### 📊 Principais Métricas
    
    - **Receita Total**: Soma de todas as receitas da unidade (Belle + Ecommerce + Parcerias)
    - **Quantidade de Atendimentos**: Número total de atendimentos únicos realizados
    - **Clientes Únicos**: Número de clientes distintos atendidos
    - **Ticket Médio**: Valor médio gasto por atendimento (Receita Belle ÷ Atendimentos Pagos)
    - **NPS (Net Promoter Score)**: Medida de satisfação e lealdade do cliente
    
    ### 💰 Fontes de Receita
    
    1. **Belle (Presencial)**
       - Atendimentos pagos diretamente na unidade
       - Inclui todos os métodos de pagamento locais
    
    2. **Ecommerce (Vouchers)**
       - Vouchers comprados no site e utilizados na unidade
       - Exclui vouchers com cupons de parceiros
    
    3. **Parcerias (Cupons)**
       - Vendas realizadas através de cupons de parceiros
       - Inclui vouchers corporativos e de afiliados
    
    ### 📅 Datas Importantes
    
    - **Data de Atendimento**: Quando o serviço foi realizado (Belle)
    - **Data de Compra (CREATED_DATE)**: Quando o voucher foi comprado (Ecommerce)
    - **Data de Uso (USED_DATE)**: Quando o voucher foi utilizado (Ecommerce)
    
    ### 🎯 Outros Termos
    
    - **Valor Líquido**: Valor recebido após dedução de taxas e impostos
    - **Valor Bruto**: Valor total antes de deduções
    - **Forma de Pagamento**: Método utilizado para efetuar o pagamento
    - **Tipo de Serviço**: Categoria do serviço prestado (Massagem, Day Spa, etc.)
    """)
    
    st.markdown("---")
    
    st.subheader("Versão do Dashboard")
    st.info("📊 **Versão Atual: 3.1** - Janeiro 2026")
    
    st.markdown("""
    ### Histórico de Versões
    
    - **v3.1 (Jan/2026)**: Adicionadas informações detalhadas das unidades (coordenador, nº de macas, cluster, salas, data inauguração)
    - **v3.0 (Dez/2025)**: Reestruturação completa do dashboard com novas abas e funcionalidades
    - **v2.5 (Out/2025)**: Melhorias na visualização de gráficos e adição de ajuda contextual
    - **v2.0 (Ago/2025)**: Integração com dados de ecommerce e parcerias
    - **v1.0 (Jun/2025)**: Lançamento inicial com métricas básicas de atendimento
    
    ### Suporte
    
    Para dúvidas ou problemas com o dashboard, entre em contato com a equipe de BI.
    """)
