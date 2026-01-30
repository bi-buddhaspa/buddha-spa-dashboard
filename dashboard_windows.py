"""
═══════════════════════════════════════════════════════════════════════════════
BUDDHA SPA - DASHBOARD DE FRANQUEADOS
Portal Analítico para Gestão de Unidades
═══════════════════════════════════════════════════════════════════════════════

VERSÃO: 3.1 - FINAL CORRIGIDO
DATA: Janeiro 2026

═══════════════════════════════════════════════════════════════════════════════
📊 COMO FUNCIONA O CÁLCULO DA RECEITA TOTAL - VERSÃO CORRIGIDA
═══════════════════════════════════════════════════════════════════════════════

A Receita Total mostrada no dashboard vem de 3 ORIGENS diferentes:

1. 🏪 BELLE (Sistema Local)
   - Atendimentos E PRODUTOS pagos DIRETAMENTE na unidade
   - Cliente chega, faz o serviço, paga no local (cartão/dinheiro/PIX)
   - INCLUI produtos vendidos (cosméticos, óleos, cremes, etc.)
   - EXCLUI TotalPass, GymPass, ClassPass (esses vão para Parcerias)
   - Registrado no sistema Belle (sistema de gestão local da unidade)
   - Exemplo: R$ 139.660,00

2. 🛒 ECOMMERCE (Vouchers - TODOS)
   - Cliente compra voucher no SITE Buddha Spa
   - INCLUI vouchers COM e SEM cupom de desconto
   - Vai na unidade e USA o voucher
   - Terapeuta atende o cliente normalmente
   - Receita conta para a unidade onde foi USADO
   - Exemplo: R$ 109.263,01 (TODOS os vouchers utilizados)

3. 🤝 PARCERIAS COMERCIAIS (TotalPass, GymPass, ClassPass)
   - Cliente usa plataforma de parceiro (TotalPass/GymPass/ClassPass)
   - Paga através da plataforma do parceiro
   - Vai na unidade e apresenta o convênio
   - Identificado pela forma_pagamento no sistema Belle:
     * "parcerias comerciais - totalpass"
     * "parcerias comerciais - gympass"
     * "parcerias comerciais - classpass"
   - Exemplo: R$ 5.000,00

RECEITA TOTAL = Belle + Ecommerce + Parcerias Comerciais
RECEITA TOTAL = R$ 139.660,00 + R$ 109.263,01 + R$ 5.000,00
RECEITA TOTAL = R$ 253.923,01 ✅

═══════════════════════════════════════════════════════════════════════════════
🔍 POR QUE OS VALORES PODEM PARECER DIFERENTES?
═══════════════════════════════════════════════════════════════════════════════

Você pode ver valores diferentes em diferentes partes do dashboard:

1. "PRINCIPAIS SERVIÇOS" mostra valor menor que Receita Total
   POR QUÊ É DIFERENTE?
   - O gráfico mostra apenas os TOP 15 serviços mais vendidos
   - Não mostra TODOS os serviços
   - É um recorte para facilitar a visualização
   - Se somar TODOS os serviços + produtos, daria a Receita Belle completa

2. "PRODUTOS ESTÃO INCLUÍDOS"
   - Antes: dashboard só contava serviços (massagens, tratamentos)
   - Agora: inclui produtos vendidos (cosméticos, óleos, cremes)
   - Isso aumenta a Receita Belle total

3. "PARCERIAS COMERCIAIS são TotalPass/GymPass/ClassPass"
   - Não são mais vouchers com cupons!
   - São atendimentos pagos via plataformas de parceiros
   - Identificados pela forma de pagamento no sistema Belle

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
🎯 MÉTRICAS PRINCIPAIS - VERSÃO CORRIGIDA
═══════════════════════════════════════════════════════════════════════════════

1. RECEITA TOTAL
   - Soma de TODAS as origens: Belle + Ecommerce + Parcerias Comerciais
   - INCLUI produtos vendidos
   - INCLUI todos os vouchers (com ou sem cupom)
   - INCLUI TotalPass, GymPass, ClassPass
   - Todo o dinheiro que entrou na unidade no período

2. QUANTIDADE DE ATENDIMENTOS - CORRIGIDO!
   - Atendimentos Belle + Vouchers Utilizados
   - Conta cada ID de venda = 1 atendimento
   - INCLUI vouchers porque geram atendimento real (terapeuta trabalha)
   - Exemplo: 1.304 atendimentos Belle + 150 vouchers = 1.454 total

3. CLIENTES ÚNICOS
   - Quantos clientes DIFERENTES foram atendidos
   - João fez 3 massagens = 3 atendimentos, mas 1 cliente único

4. TICKET MÉDIO - CORRIGIDO!
   - Receita TOTAL ÷ Atendimentos TOTAIS (incluindo vouchers)
   - Usa TUDO porque vouchers geram receita via reembolso
   - Mede: "Quanto de receita média por atendimento?"
   - Exemplo: R$ 253.923 ÷ 1.454 = R$ 174,60 por atendimento

═══════════════════════════════════════════════════════════════════════════════
✅ MUDANÇAS IMPLEMENTADAS NESTA VERSÃO
═══════════════════════════════════════════════════════════════════════════════

1. ✅ PRODUTOS INCLUÍDOS - removido filtro tipo_item = 'Serviço'
2. ✅ PARCERIAS COMERCIAIS = TotalPass/GymPass/ClassPass (forma_pagamento)
3. ✅ ECOMMERCE = TODOS os vouchers (com ou sem cupom)
4. ✅ QUANTIDADE ATENDIMENTOS = Belle + Vouchers utilizados
5. ✅ TICKET MÉDIO = Receita Total ÷ Total Atendimentos

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
# CARREGAR DADOS DE ECOMMERCE PARA CALCULAR RECEITA TOTAL
# -----------------------------------------------------------------------------
with st.spinner("Calculando faturamento total..."):
    try:
        unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
        df_ecom_fat = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
    except Exception as e:
        st.error(f"Erro ao carregar ecommerce: {e}")
        df_ecom_fat = pd.DataFrame()

# -----------------------------------------------------------------------------
# NOVA LÓGICA - SEPARAR PARCERIAS COMERCIAIS
# 
# PARCERIAS COMERCIAIS = TotalPass, GymPass, ClassPass
# Identificados pela forma_pagamento contendo "parcerias comerciais"
# 
# BELLE = Todo o resto (dinheiro, PIX, cartão, etc) - INCLUI produtos
# 
# ECOMMERCE = TODOS os vouchers (com ou sem cupom)
# -----------------------------------------------------------------------------

# PASSO 1: Identificar e separar Parcerias Comerciais
PARCERIAS_COMERCIAIS = [
    'parcerias comerciais - totalpass',
    'parcerias comerciais - gympass',
    'parcerias comerciais - classpass'
]

df['forma_pagamento_lower'] = df['forma_pagamento'].str.lower()
df_parcerias_comerciais = df[df['forma_pagamento_lower'].isin(PARCERIAS_COMERCIAIS)]
df_belle_puro = df[~df['forma_pagamento_lower'].isin(PARCERIAS_COMERCIAIS)]

# PASSO 2: Calcular receitas por origem
receita_belle = df_belle_puro[valor_col].sum()  # Belle SEM parcerias comerciais
receita_parcerias_comerciais = df_parcerias_comerciais[valor_col].sum()  # Apenas TotalPass/GymPass/ClassPass

# PASSO 3: Ecommerce = TODOS os vouchers (com ou sem cupom)
receita_ecommerce = 0
qtd_vouchers_utilizados = 0

if not df_ecom_fat.empty:
    df_ecom_fat['PRICE_NET'] = pd.to_numeric(df_ecom_fat['PRICE_NET'], errors='coerce')
    receita_ecommerce = df_ecom_fat['PRICE_NET'].fillna(0).sum()
    qtd_vouchers_utilizados = len(df_ecom_fat)

# PASSO 4: RECEITA TOTAL = Belle + Ecommerce + Parcerias Comerciais
receita_total = receita_belle + receita_ecommerce + receita_parcerias_comerciais

print(f"""
═══════════════════════════════════════════════
CÁLCULO DA RECEITA TOTAL - CORRIGIDO
═══════════════════════════════════════════════
Belle (Sistema Local):         {formatar_moeda(receita_belle)}
Ecommerce (Todos Vouchers):    {formatar_moeda(receita_ecommerce)}
Parcerias Comerciais:          {formatar_moeda(receita_parcerias_comerciais)}
───────────────────────────────────────────────
RECEITA TOTAL:                 {formatar_moeda(receita_total)}
═══════════════════════════════════════════════
""")

# -----------------------------------------------------------------------------
# HEADER / KPIs
# -----------------------------------------------------------------------------
col_logo, col_title = st.columns([1, 5])

with col_logo:
    st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTkFQZ7QiSmOpEWC_9Ndsuqx_-roUMRJJkCvw&s", width=200)

with col_title:
    st.title("Buddha Spa - Dashboard de Unidades")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

# MÉTRICAS CORRIGIDAS
qtd_atendimentos_belle = int(df['id_venda'].nunique())
qtd_atendimentos_total = qtd_atendimentos_belle + qtd_vouchers_utilizados  # INCLUI vouchers

qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0

# TICKET MÉDIO CORRIGIDO: Receita TOTAL / Atendimentos TOTAIS (incluindo vouchers)
ticket_medio = receita_total / qtd_atendimentos_total if qtd_atendimentos_total > 0 else 0

# KPIs PRINCIPAIS COM AJUDA
colk1, colk2, colk3, colk4 = st.columns(4)

with colk1:
    st.metric("Receita Total", formatar_moeda(receita_total))
    with st.popover("ℹ️"):
        st.caption("Belle + Ecommerce + Parcerias Comerciais. INCLUI produtos vendidos.")

with colk2:
    st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos_total))
    with st.popover("ℹ️"):
        st.caption(f"Belle: {formatar_numero(qtd_atendimentos_belle)} + Vouchers: {formatar_numero(qtd_vouchers_utilizados)} = Total: {formatar_numero(qtd_atendimentos_total)}")

with colk3:
    st.metric("Clientes Únicos", formatar_numero(qtd_clientes))
    with st.popover("ℹ️"):
        st.caption("Número de clientes distintos que foram atendidos no período. Um mesmo cliente pode ter feito múltiplos atendimentos.")

with colk4:
    st.metric("Ticket Médio por Atendimento", formatar_moeda(ticket_medio))
    with st.popover("ℹ️"):
        st.caption(f"Receita Total ({formatar_moeda(receita_total)}) ÷ Total Atendimentos ({formatar_numero(qtd_atendimentos_total)}). INCLUI vouchers porque a unidade recebe reembolso.")

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
       - Atendimentos e PRODUTOS pagos diretamente na unidade
       - INCLUI produtos vendidos (cosméticos, óleos, etc.)
       - Formas de pagamento: dinheiro, cartão, PIX, etc.
       - EXCLUI: TotalPass, GymPass, ClassPass (esses vão para Parcerias)
    
    2. **🛒 Ecommerce (Vouchers): {formatar_moeda(receita_ecommerce)}**
       - TODOS os vouchers comprados online e utilizados na unidade
       - INCLUI vouchers COM e SEM cupom de desconto
       - {formatar_numero(qtd_vouchers_utilizados)} vouchers utilizados no período
    
    3. **🤝 Parcerias Comerciais: {formatar_moeda(receita_parcerias_comerciais)}**
       - TotalPass, GymPass, ClassPass
       - Identificado pela forma de pagamento no sistema Belle
    
    #### 📍 O que está incluído:
    
    **Atendimentos Pagos (Belle)**
    - SERVIÇOS realizados e pagos na unidade
    - PRODUTOS vendidos (cosméticos, óleos, cremes, etc.)
    - Apenas o **valor líquido** (já descontado impostos e taxas)
    
    **Vouchers Utilizados (Ecommerce)**
    - Vouchers comprados no site e utilizados na sua unidade
    - Baseado na data de utilização (USED_DATE)
    - Incluindo vouchers com cupons de desconto
    
    **Parcerias Comerciais**
    - Atendimentos via TotalPass, GymPass, ClassPass
    - Identificado pela forma de pagamento
    
    #### 🔍 Detalhamento:
    
    - **Período**: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
    - **Atendimentos Belle**: {formatar_numero(qtd_atendimentos_belle)}
    - **Vouchers utilizados**: {formatar_numero(qtd_vouchers_utilizados)}
    - **TOTAL de atendimentos**: {formatar_numero(qtd_atendimentos_total)}
    - **Clientes únicos**: {formatar_numero(qtd_clientes)}
    - **Ticket médio**: {formatar_moeda(ticket_medio)}
    
    #### ✅ INCLUÍDO na receita:
    - Serviços prestados
    - Produtos vendidos
    - Vouchers utilizados (com ou sem cupom)
    - Parcerias comerciais (TotalPass, GymPass, ClassPass)
    
    #### ❌ NÃO incluído:
    - Vouchers vendidos mas ainda não utilizados
    - Vendas canceladas ou reembolsadas
    
    #### 💡 Quer ver mais detalhes?
    
    - **Aba Financeiro**: Veja a distribuição completa por origem
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
        st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos_total))
        with st.popover("ℹ️"):
            st.caption(f"Total de atendimentos: Belle ({formatar_numero(qtd_atendimentos_belle)}) + Vouchers ({formatar_numero(qtd_vouchers_utilizados)}) = {formatar_numero(qtd_atendimentos_total)}")
    
    with colf3:
        st.metric("Ticket Médio Unidade", formatar_moeda(ticket_medio))
        with st.popover("ℹ️"):
            st.caption("Valor médio por atendimento na unidade (Belle).")
    
    st.markdown("---")
    
    # Faturamento Detalhado por Origem
    col_titulo_fat, col_ajuda_fat = st.columns([0.97, 0.03])
    with col_titulo_fat:
        st.subheader("Faturamento Detalhado por Origem")
    with col_ajuda_fat:
        with st.popover("ℹ️"):
            st.markdown("""
            **Origens do Faturamento:**
            
            - **Belle (Sistema Local)**: Vendas registradas no sistema de gestão da unidade (INCLUI produtos)
            - **Ecommerce (Vouchers)**: Todos os vouchers utilizados (com ou sem cupom)
            - **Parcerias Comerciais**: TotalPass, GymPass, ClassPass
            """)
    
    faturamento_total_completo = receita_belle + receita_ecommerce + receita_parcerias_comerciais
    
    # Cards de faturamento
    col_fat1, col_fat2, col_fat3, col_fat4 = st.columns(4)
    
    with col_fat1:
        st.metric("💰 Faturamento Total", formatar_moeda(faturamento_total_completo))
        with st.popover("ℹ️"):
            st.caption("Soma de todas as receitas: Belle + Ecommerce + Parcerias Comerciais")
    
    with col_fat2:
        st.metric("🏪 Belle (Sistema Local)", formatar_moeda(receita_belle))
        with st.popover("ℹ️"):
            st.caption("Atendimentos e produtos pagos diretamente na unidade (dinheiro, cartão, PIX). INCLUI produtos vendidos.")
    
    with col_fat3:
        st.metric("🛒 Ecommerce (Vouchers)", formatar_moeda(receita_ecommerce))
        with st.popover("ℹ️"):
            st.caption(f"Todos os vouchers utilizados (com ou sem cupom). Total: {formatar_numero(qtd_vouchers_utilizados)} vouchers")
    
    with col_fat4:
        st.metric("🤝 Parcerias Comerciais", formatar_moeda(receita_parcerias_comerciais))
        with st.popover("ℹ️"):
            st.caption("TotalPass, GymPass, ClassPass (identificado pela forma de pagamento)")
    
    # Gráficos de distribuição
    df_faturamento = pd.DataFrame({
        'Origem': ['Belle\n(Sistema Local)', 'Ecommerce\n(Todos Vouchers)', 'Parcerias\nComerciais'],
        'Receita': [receita_belle, receita_ecommerce, receita_parcerias_comerciais],
        'Percentual': [
            (receita_belle / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0,
            (receita_ecommerce / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0,
            (receita_parcerias_comerciais / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0
        ]
    })
    
    col_graf_fat1, col_graf_fat2 = st.columns([2, 1])
    
    with col_graf_fat1:
        fig_fat = px.bar(
            df_faturamento,
            x='Origem',
            y='Receita',
            text=df_faturamento['Percentual'].apply(lambda x: f"{x:.1f}%"),
            labels={'Receita': 'Receita (R$)', 'Origem': 'Origem da Receita'},
            color='Origem',
            color_discrete_map={
                'Belle\n(Sistema Local)': '#8B0000',
                'Ecommerce\n(Todos Vouchers)': '#CD5C5C',
                'Parcerias\nComerciais': '#F08080'
            }
        )
        
        fig_fat.update_traces(textposition='outside', textfont=dict(size=12, color='#8B0000'))
        fig_fat.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=400,
            showlegend=False
        )
        fig_fat.update_yaxes(tickformat=",.2f")
        
        st.plotly_chart(fig_fat, use_container_width=True, key="chart_faturamento_origem")
    
    with col_graf_fat2:
        fig_fat_pie = px.pie(
            df_faturamento,
            names='Origem',
            values='Receita',
            color='Origem',
            color_discrete_map={
                'Belle\n(Sistema Local)': '#8B0000',
                'Ecommerce\n(Todos Vouchers)': '#CD5C5C',
                'Parcerias\nComerciais': '#F08080'
            }
        )
        
        fig_fat_pie.update_traces(textposition='inside', textinfo='percent')
        fig_fat_pie.update_layout(
            paper_bgcolor='#F5F0E6',
            height=400,
            showlegend=True,
            legend=dict(orientation="v", yanchor="middle", y=0.5)
        )
        
        st.plotly_chart(fig_fat_pie, use_container_width=True, key="chart_faturamento_pie")
    
    # Tabela detalhada com totalizador
    st.markdown("#### Detalhamento por Origem")
    
    df_fat_tabela = df_faturamento.copy()
    df_fat_tabela['Receita_fmt'] = df_fat_tabela['Receita'].apply(formatar_moeda)
    df_fat_tabela['Percentual_fmt'] = df_fat_tabela['Percentual'].apply(lambda x: formatar_percentual(x))
    
    # Adicionar linha de total
    total_row = {
        'Origem': 'TOTAL',
        'Receita': faturamento_total_completo,
        'Percentual': 100.0,
        'Receita_fmt': formatar_moeda(faturamento_total_completo),
        'Percentual_fmt': '100,00%'
    }
    
    df_fat_tabela = pd.concat([df_fat_tabela, pd.DataFrame([total_row])], ignore_index=True)
    
    st.dataframe(
        df_fat_tabela[['Origem', 'Receita_fmt', 'Percentual_fmt']].rename(columns={
            'Origem': 'Origem da Receita',
            'Receita_fmt': 'Receita',
            'Percentual_fmt': '% do Total'
        }),
        use_container_width=True,
        height=250
    )
    
    st.markdown("---")
    
    # ═════════════════════════════════════════════════════════════════════════
    # TABELA DETALHADA DE RECEITAS POR FORMA DE PAGAMENTO
    # 
    # ESTA TABELA REPLICA O RELATÓRIO DO BI ANTIGO
    # Mostra cada unidade com todas as formas de pagamento separadas em colunas
    # 
    # ESTRUTURA:
    # - Linhas: Unidades
    # - Colunas: Formas de Pagamento (Dinheiro, PIX, Cartão, Voucher, etc.)
    # - Última linha: TOTAL de cada forma de pagamento
    # 
    # DADOS USADOS:
    # - df_detalhado: contém cada atendimento com sua forma de pagamento
    # - Agrupa por: unidade + forma_pagamento
    # - Pivota: forma_pagamento vira coluna
    # ═════════════════════════════════════════════════════════════════════════
    
    col_titulo_detalhe, col_ajuda_detalhe = st.columns([0.97, 0.03])
    with col_titulo_detalhe:
        st.subheader("📊 Receitas Detalhadas por Forma de Pagamento")
    with col_ajuda_detalhe:
        with st.popover("ℹ️"):
            st.markdown("""
            **Tabela Completa de Receitas**
            
            Esta tabela mostra TODAS as formas de pagamento separadas por coluna, 
            igual ao relatório do Business Intelligence.
            
            **Como ler:**
            - Cada linha = uma unidade
            - Cada coluna = uma forma de pagamento
            - Última linha = TOTAL geral de cada forma de pagamento
            
            **Formas de Pagamento:**
            - Dinheiro
            - PIX
            - Cartão de Crédito
            - Cartão de Débito
            - Voucher
            - Transferência
            - Outras formas
            """)
    
    # PASSO 1: Criar tabela pivotada
    # Agrupa df_detalhado por unidade e forma_pagamento, soma os valores
    if 'forma_pagamento' in df_detalhado.columns:
        df_pivot_prep = (
            df_detalhado.groupby(['unidade', 'forma_pagamento'])[valor_col]
            .sum()
            .reset_index()
        )
        
        # PASSO 2: Pivotar - transformar forma_pagamento em colunas
        # Antes: 
        #   unidade         | forma_pagamento | valor
        #   Higienópolis    | Dinheiro        | 10000
        #   Higienópolis    | PIX             | 15000
        # 
        # Depois:
        #   unidade         | Dinheiro | PIX   | Total
        #   Higienópolis    | 10000    | 15000 | 25000
        df_pivot = df_pivot_prep.pivot_table(
            index='unidade',
            columns='forma_pagamento',
            values=valor_col,
            aggfunc='sum',
            fill_value=0  # Preencher vazios com 0
        ).reset_index()
        
        # PASSO 3: Adicionar coluna de TOTAL (soma de todas as formas de pagamento)
        # Pega todas as colunas exceto 'unidade' e soma
        formas_pagamento_colunas = [col for col in df_pivot.columns if col != 'unidade']
        df_pivot['TOTAL'] = df_pivot[formas_pagamento_colunas].sum(axis=1)
        
        # PASSO 4: Ordenar por TOTAL (maior receita no topo)
        df_pivot = df_pivot.sort_values('TOTAL', ascending=False)
        
        # PASSO 5: Adicionar linha de TOTAL GERAL
        # Soma cada coluna para criar a linha de totalizador
        total_row = {'unidade': 'TOTAL GERAL'}
        for col in formas_pagamento_colunas:
            total_row[col] = df_pivot[col].sum()
        total_row['TOTAL'] = df_pivot['TOTAL'].sum()
        
        df_pivot = pd.concat([df_pivot, pd.DataFrame([total_row])], ignore_index=True)
        
        # PASSO 6: Formatar valores para exibição (R$ 1.234,56)
        df_pivot_display = df_pivot.copy()
        
        # Formatar unidade (title case)
        df_pivot_display['unidade'] = df_pivot_display['unidade'].apply(
            lambda x: x.title() if x != 'TOTAL GERAL' else x
        )
        
        # Formatar todas as colunas numéricas como moeda
        for col in formas_pagamento_colunas + ['TOTAL']:
            df_pivot_display[col] = df_pivot_display[col].apply(
                lambda x: formatar_moeda(x) if pd.notna(x) else 'R$ 0,00'
            )
        
        # PASSO 7: Renomear colunas para nomes amigáveis
        rename_dict = {
            'unidade': 'Unidade',
            'TOTAL': '💰 TOTAL'
        }
        
        # Manter nomes originais das formas de pagamento mas com primeira letra maiúscula
        for col in formas_pagamento_colunas:
            rename_dict[col] = col.title()
        
        df_pivot_display = df_pivot_display.rename(columns=rename_dict)
        
        # PASSO 8: Exibir tabela
        st.dataframe(
            df_pivot_display,
            use_container_width=True,
            height=600  # Altura maior para ver todas as unidades
        )
        
        # PASSO 9: Adicionar botão de download CSV
        # Criar CSV com valores numéricos (não formatados) para análise externa
        csv_export = df_pivot.copy()
        csv_export['unidade'] = csv_export['unidade'].apply(lambda x: x.title() if x != 'TOTAL GERAL' else x)
        csv_data = csv_export.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
        
        st.download_button(
            label="📥 Download Tabela Completa (CSV)",
            data=csv_data,
            file_name=f"receitas_detalhadas_{data_inicio.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.csv",
            mime="text/csv",
            key="download_receitas_detalhadas"
        )
        
        # PASSO 10: Adicionar análise resumida
        st.markdown("#### 📈 Análise Rápida")
        
        col_analise1, col_analise2, col_analise3 = st.columns(3)
        
        # Calcular forma de pagamento mais usada
        # (excluindo a linha TOTAL GERAL)
        df_analise = df_pivot[df_pivot['unidade'] != 'TOTAL GERAL'].copy()
        totais_por_forma = {}
        for col in formas_pagamento_colunas:
            totais_por_forma[col] = df_analise[col].sum()
        
        forma_mais_usada = max(totais_por_forma, key=totais_por_forma.get)
        valor_forma_mais_usada = totais_por_forma[forma_mais_usada]
        
        # Calcular percentual da forma mais usada
        total_geral = sum(totais_por_forma.values())
        perc_forma_mais_usada = (valor_forma_mais_usada / total_geral * 100) if total_geral > 0 else 0
        
        with col_analise1:
            st.metric(
                "💳 Forma de Pagamento Mais Usada",
                forma_mais_usada.title(),
                f"{formatar_percentual(perc_forma_mais_usada)} do total"
            )
        
        with col_analise2:
            st.metric(
                "💰 Receita desta Forma",
                formatar_moeda(valor_forma_mais_usada)
            )
        
        with col_analise3:
            # Quantidade de formas de pagamento ativas
            formas_ativas = sum(1 for v in totais_por_forma.values() if v > 0)
            st.metric(
                "📊 Formas de Pagamento Ativas",
                formas_ativas
            )
    else:
        st.warning("⚠️ Coluna 'forma_pagamento' não encontrada nos dados. Não é possível gerar o detalhamento.")
    
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
            # Adicionar totalizador
            df_serv_fin_com_total = adicionar_totalizador(
                df_serv_fin,
                colunas_numericas=['receita', 'qtd'],
                primeira_coluna='nome_servico_simplificado'
            )
            
            df_serv_fin_display = df_serv_fin_com_total.copy()
            df_serv_fin_display['receita_fmt'] = df_serv_fin_display['receita'].apply(
                lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
            )
            df_serv_fin_display['qtd_fmt'] = df_serv_fin_display['qtd'].apply(
                lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
            )
            
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
    
    col_titulo_vouchers_fin, col_ajuda_vouchers_fin = st.columns([0.97, 0.03])
    with col_titulo_vouchers_fin:
        st.subheader("Vouchers Mais Utilizados na Unidade")
    with col_ajuda_vouchers_fin:
        with st.popover("ℹ️"):
            st.caption("Top 10 pacotes/serviços de vouchers que foram mais utilizados na sua unidade, ordenados por receita líquida.")
    
    if not df_ecom_fat.empty:
        if 'PACKAGE_NAME' in df_ecom_fat.columns:
            df_ecom_fat['PACKAGE_NAME'] = df_ecom_fat['PACKAGE_NAME'].fillna(df_ecom_fat['NAME'])
        else:
            df_ecom_fat['PACKAGE_NAME'] = df_ecom_fat['NAME']
        
        df_ecom_top = (
            df_ecom_fat
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
            # Adicionar totalizador
            df_ecom_top_com_total = adicionar_totalizador(
                df_ecom_top,
                colunas_numericas=['qtde_vouchers', 'receita_liquida'],
                primeira_coluna='PACKAGE_NAME'
            )
            
            df_ecom_top_display = df_ecom_top_com_total.copy()
            df_ecom_top_display['qtde_vouchers_fmt'] = df_ecom_top_display['qtde_vouchers'].apply(
                lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
            )
            df_ecom_top_display['receita_liquida_fmt'] = df_ecom_top_display['receita_liquida'].apply(
                lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
            )
            
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
    st.info("📌 **Nota para Franqueados**: Esta aba mostra apenas dados de ecommerce relacionados à sua unidade. Dados de marketing da rede (site, redes sociais, anúncios) são gerenciados pela holding e não aparecem aqui.")
    
    st.markdown("---")
    
    # BLOCO 1 – ECOMMERCE
    col_titulo_ecom, col_ajuda_ecom = st.columns([0.97, 0.03])
    with col_titulo_ecom:
        st.subheader("Ecommerce – Vouchers Utilizados na Unidade")
    with col_ajuda_ecom:
        with st.popover("ℹ️"):
            st.markdown("""
            **Importante sobre Vouchers:**
            
            Os vouchers são vendidos no site geral do Buddha Spa e podem ser 
            utilizados em qualquer unidade da rede.
            
            **Este dashboard mostra apenas:**
            - Vouchers que foram **utilizados** na sua unidade
            - Data considerada: quando o cliente **usou** o voucher (USED_DATE)
            - Não mostra vouchers vendidos mas ainda não utilizados
            
            **AFILLIATION_NAME** indica em qual unidade o voucher foi usado.
            """)
    
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
        
        with colm1:
            st.metric("Total de Vendas", formatar_numero(total_pedidos))
            with st.popover("ℹ️"):
                st.caption("Número de pedidos (compras) cujos vouchers foram utilizados na sua unidade no período.")
        
        with colm2:
            st.metric("Vouchers Utilizados", formatar_numero(total_vouchers))
            with st.popover("ℹ️"):
                st.caption("Total de vouchers usados. Um pedido pode ter múltiplos vouchers (ex: pacote com 4 sessões).")
        
        with colm3:
            st.metric("Receita Vouchers Utilizados", formatar_moeda(receita_liquida_e))
            with st.popover("ℹ️"):
                st.caption("Valor líquido dos vouchers utilizados (após descontos e cupons).")
        
        with colm4:
            st.metric("Ticket Médio Vendas", formatar_moeda(ticket_medio_e))
            with st.popover("ℹ️"):
                st.caption("Valor médio por pedido: Receita Total ÷ Número de Pedidos.")
        
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
            df_serv_display = df_ecom.copy()
            # Formatar receita (manter NULL como está, só formatar os valores válidos)
            df_serv_display['receita_fmt'] = df_serv_display['PRICE_NET'].apply(
                lambda x: formatar_moeda(x) if pd.notna(x) else ''
            )
            if 'USED_DATE_BRAZIL' in df_serv_display.columns:
                df_serv_display = df_serv_display.sort_values('USED_DATE_BRAZIL', ascending=False)
            
            # RESETAR O ÍNDICE para ficar sequencial de 0 até N
            df_serv_display = df_serv_display.reset_index(drop=True)
            
            # Mostrar informação de debug
            st.caption(f"📊 Total de vouchers na tabela: {len(df_serv_display)}")

            # Preparar colunas (converter NaN para string vazia para exibição)
            df_display_final = df_serv_display[['KEY', 'ORDER_ID', 'PACKAGE_NAME', 'receita_fmt']].copy()
            df_display_final = df_display_final.fillna('')  # Agora sim, converte NULL para string vazia
            
            st.dataframe(
                df_display_final.rename(columns={
                    'KEY': 'ID Voucher',
                    'ORDER_ID': 'ID Venda',
                    'PACKAGE_NAME': 'Serviço / Pacote',
                    'receita_fmt': 'Receita Líquida'
                }),
                use_container_width=True,
                height=450
            )
        
        st.markdown("---")
        
        # Análise Geográfica
        col_titulo_geo, col_ajuda_geo = st.columns([0.97, 0.03])
        with col_titulo_geo:
            st.subheader("Distribuição Geográfica - Vendas por Estado")
        with col_ajuda_geo:
            with st.popover("ℹ️"):
                st.caption("Estados de onde vieram os clientes que utilizaram vouchers na sua unidade. Baseado no endereço de cobrança do pedido.")
        
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
    
    # BLOCO 1.5 – VOUCHERS OMNICHANNEL (NOVA SEÇÃO)
    col_titulo_omni, col_ajuda_omni = st.columns([0.97, 0.03])
    with col_titulo_omni:
        st.subheader("📊 Vouchers Omnichannel ")
    with col_ajuda_omni:
        with st.popover("ℹ️"):
            st.markdown("""
            **Vouchers Omnichannel**
            
            Visão completa de TODOS os vouchers vendidos para sua(s) unidade(s) no período selecionado.
            
            **Diferença para seção anterior:**
            - Seção anterior: apenas vouchers **utilizados** (USED_DATE)
            - Esta seção: todos os vouchers **vendidos** (CREATED_DATE)
            
            **Data considerada:** CREATED_DATE (data da compra do voucher)
            """)
    
    with st.spinner("Carregando vouchers omnichannel..."):
        try:
            unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
            df_omni = load_omnichannel_vouchers(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar vouchers omnichannel: {e}")
            df_omni = pd.DataFrame()
    
    if df_omni.empty:
        st.warning("Sem dados de vouchers omnichannel para o período selecionado.")
    else:
        # Converter colunas numéricas
        df_omni['PRICE_GROSS'] = pd.to_numeric(df_omni['PRICE_GROSS'], errors='coerce')
        df_omni['PRICE_NET'] = pd.to_numeric(df_omni['PRICE_NET'], errors='coerce')
        df_omni['PRICE_REFOUND'] = pd.to_numeric(df_omni['PRICE_REFOUND'], errors='coerce')
        
        # Completar PACKAGE_NAME
        if 'PACKAGE_NAME' in df_omni.columns:
            df_omni['PACKAGE_NAME'] = df_omni['PACKAGE_NAME'].fillna(df_omni['NAME'])
        else:
            df_omni['PACKAGE_NAME'] = df_omni['NAME']
        
        # KPIs Principais
        col_omni1, col_omni2, col_omni3, col_omni4 = st.columns(4)
        
        total_vouchers_vendidos = len(df_omni)
        total_pedidos_vendidos = int(df_omni['ORDER_ID'].nunique())
        receita_bruta_omni = df_omni['PRICE_GROSS'].fillna(0).sum()
        receita_liquida_omni = df_omni['PRICE_NET'].fillna(0).sum()
        
        with col_omni1:
            st.metric("Total de Vouchers Vendidos", formatar_numero(total_vouchers_vendidos))
            with st.popover("ℹ️"):
                st.caption("Total de vouchers vendidos para sua(s) unidade(s) no período, independente se foram usados ou não.")
        
        with col_omni2:
            st.metric("Total de Pedidos", formatar_numero(total_pedidos_vendidos))
            with st.popover("ℹ️"):
                st.caption("Número de pedidos únicos. Um pedido pode conter múltiplos vouchers.")
        
        with col_omni3:
            st.metric("Receita Bruta", formatar_moeda(receita_bruta_omni))
            with st.popover("ℹ️"):
                st.caption("Valor total antes de descontos e cupons.")
        
        with col_omni4:
            st.metric("Receita Líquida", formatar_moeda(receita_liquida_omni))
            with st.popover("ℹ️"):
                st.caption("Valor efetivamente recebido após descontos e cupons.")
        
        st.markdown("---")
        
        # Gráficos - REMOVENDO GRÁFICO DE STATUS
        col_omni_g1 = st.columns(1)[0]
        
        with col_omni_g1:
            st.markdown("#### Vouchers por Unidade")
            if 'AFILLIATION_NAME' in df_omni.columns:
                df_unidade_omni = (
                    df_omni.groupby('AFILLIATION_NAME')
                    .agg(qtd=('ID', 'count'), receita=('PRICE_NET', 'sum'))
                    .reset_index()
                    .sort_values('receita', ascending=False)
                    .head(10)
                )
                
                # Adicionar totalizador
                df_unidade_omni_com_total = adicionar_totalizador(
                    df_unidade_omni,
                    colunas_numericas=['qtd', 'receita'],
                    primeira_coluna='AFILLIATION_NAME'
                )
                
                # Formatar para exibição no gráfico (sem a linha TOTAL)
                df_unidade_omni_grafico = df_unidade_omni.copy()
                df_unidade_omni_grafico['receita_fmt'] = df_unidade_omni_grafico['receita'].apply(formatar_moeda)
                
                fig_unidade_omni = px.bar(
                    df_unidade_omni_grafico,
                    x='receita',
                    y='AFILLIATION_NAME',
                    orientation='h',
                    text='receita_fmt',
                    labels={'receita': 'Receita (R$)', 'AFILLIATION_NAME': 'Unidade'}
                )
                fig_unidade_omni.update_yaxes(autorange='reversed')
                fig_unidade_omni.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
                fig_unidade_omni.update_layout(
                    plot_bgcolor='#FFFFFF',
                    paper_bgcolor='#F5F0E6',
                    height=350,
                    yaxis={'categoryorder': 'total descending'}
                )
                st.plotly_chart(fig_unidade_omni, use_container_width=True, key="chart_unidade_omni")
        
        st.markdown("---")
        
        # Top Produtos Vendidos
        st.markdown("#### Top 10 Produtos Mais Vendidos (Omnichannel)")
        
        df_produtos_omni = (
            df_omni.groupby('PACKAGE_NAME')
            .agg(
                qtd_vouchers=('ID', 'count'),
                receita_bruta=('PRICE_GROSS', 'sum'),
                receita_liquida=('PRICE_NET', 'sum')
            )
            .reset_index()
            .sort_values('qtd_vouchers', ascending=False)
            .head(10)
        )
        
        col_prod_omni1, col_prod_omni2 = st.columns([2, 1])
        
        with col_prod_omni1:
            df_produtos_omni['qtd_fmt_label'] = df_produtos_omni['qtd_vouchers'].apply(formatar_numero)
            
            fig_prod_omni = px.bar(
                df_produtos_omni,
                x='qtd_vouchers',
                y='PACKAGE_NAME',
                orientation='h',
                text='qtd_fmt_label',
                labels={'qtd_vouchers': 'Quantidade Vendida', 'PACKAGE_NAME': 'Produto'}
            )
            fig_prod_omni.update_yaxes(autorange='reversed')
            fig_prod_omni.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
            fig_prod_omni.update_layout(
                plot_bgcolor='#FFFFFF',
                paper_bgcolor='#F5F0E6',
                height=450,
                yaxis={'categoryorder': 'total descending'}
            )
            st.plotly_chart(fig_prod_omni, use_container_width=True, key="chart_produtos_omni")
        
        with col_prod_omni2:
            # Adicionar totalizador
            df_produtos_omni_com_total = adicionar_totalizador(
                df_produtos_omni,
                colunas_numericas=['qtd_vouchers', 'receita_bruta', 'receita_liquida'],
                primeira_coluna='PACKAGE_NAME'
            )
            
            df_produtos_omni_display = df_produtos_omni_com_total.copy()
            df_produtos_omni_display['qtd_vouchers_fmt'] = df_produtos_omni_display['qtd_vouchers'].apply(
                lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
            )
            df_produtos_omni_display['receita_bruta_fmt'] = df_produtos_omni_display['receita_bruta'].apply(
                lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
            )
            df_produtos_omni_display['receita_liquida_fmt'] = df_produtos_omni_display['receita_liquida'].apply(
                lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
            )
            
            st.dataframe(
                df_produtos_omni_display[['PACKAGE_NAME', 'qtd_vouchers_fmt', 'receita_bruta_fmt', 'receita_liquida_fmt']].rename(columns={
                    'PACKAGE_NAME': 'Produto',
                    'qtd_vouchers_fmt': 'Qtd',
                    'receita_bruta_fmt': 'R$ Bruto',
                    'receita_liquida_fmt': 'R$ Líquido'
                }),
                use_container_width=True,
                height=450
            )
        
        st.markdown("---")
        
        # Análise de Cupons
        if 'COUPONS' in df_omni.columns:
            st.markdown("#### Performance de Cupons de Desconto")
            
            df_com_cupom_omni = df_omni[df_omni['COUPONS'].notna() & (df_omni['COUPONS'] != '')]
            
            if not df_com_cupom_omni.empty:
                col_cup_omni1, col_cup_omni2, col_cup_omni3 = st.columns(3)
                
                vouchers_com_cupom_omni = len(df_com_cupom_omni)
                perc_com_cupom_omni = (vouchers_com_cupom_omni / total_vouchers_vendidos * 100) if total_vouchers_vendidos > 0 else 0
                receita_cupom_omni = df_com_cupom_omni['PRICE_NET'].sum()
                desconto_total_omni = df_com_cupom_omni['PRICE_GROSS'].sum() - df_com_cupom_omni['PRICE_NET'].sum()
                
                with col_cup_omni1:
                    st.metric("Vouchers com Cupom", f"{formatar_numero(vouchers_com_cupom_omni)} ({formatar_percentual(perc_com_cupom_omni)})")
                
                with col_cup_omni2:
                    st.metric("Receita com Cupons", formatar_moeda(receita_cupom_omni))
                
                with col_cup_omni3:
                    st.metric("Desconto Total Aplicado", formatar_moeda(desconto_total_omni))
                
                # Top cupons
                df_cupons_omni = (
                    df_com_cupom_omni.groupby('COUPONS')
                    .agg(
                        qtd_usos=('ID', 'count'),
                        receita=('PRICE_NET', 'sum'),
                        desconto_calc=('PRICE_GROSS', 'sum')
                    )
                    .reset_index()
                )
                df_cupons_omni['desconto'] = df_cupons_omni['desconto_calc'] - df_cupons_omni['receita']
                df_cupons_omni = df_cupons_omni.sort_values('qtd_usos', ascending=False).head(10)
                
                df_cupons_omni['qtd_usos_fmt'] = df_cupons_omni['qtd_usos'].apply(formatar_numero)
                
                fig_cupons_omni = px.bar(
                    df_cupons_omni,
                    x='qtd_usos',
                    y='COUPONS',
                    orientation='h',
                    text='qtd_usos_fmt',
                    labels={'qtd_usos': 'Quantidade de Usos', 'COUPONS': 'Cupom'}
                )
                fig_cupons_omni.update_yaxes(autorange='reversed')
                fig_cupons_omni.update_traces(marker_color='#8B0000', textposition='inside', textfont=dict(color='white', size=11))
                fig_cupons_omni.update_layout(
                    plot_bgcolor='#FFFFFF',
                    paper_bgcolor='#F5F0E6',
                    height=400,
                    yaxis={'categoryorder': 'total descending'}
                )
                st.plotly_chart(fig_cupons_omni, use_container_width=True, key="chart_cupons_omni")
            else:
                st.info("Nenhum cupom foi utilizado no período.")

# ---------------------- TAB: SELF-SERVICE -------------------------
with tab_selfservice:
    col_titulo_self, col_ajuda_self = st.columns([0.97, 0.03])
    with col_titulo_self:
        st.subheader("Monte Sua Própria Análise")
    with col_ajuda_self:
        with st.popover("ℹ️"):
            st.caption("Crie análises personalizadas selecionando as dimensões (como agrupar) e métricas (o que calcular). Ideal para extrair relatórios específicos.")
    
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
    
    **Receita Total** – Soma de todas as receitas: Belle (Sistema Local) + Ecommerce (Vouchers) + Parcerias Comerciais (TotalPass, GymPass, ClassPass).
    
    **Receita Belle** – Soma de todos os valores líquidos de atendimentos presenciais no período.
    
    **Quantidade de Atendimentos** – Número de atendimentos únicos (`id_venda`).
    
    **Clientes Únicos** – Número de clientes distintos atendidos.
    
    **Ticket Médio por Atendimento** – Receita Belle ÷ Quantidade de Atendimentos.
    
    **NPS (Net Promoter Score)** – Indicador de satisfação calculado como: (% Promotores - % Detratores).
    - **Promotores**: Notas 9-10
    - **Neutros**: Notas 7-8
    - **Detratores**: Notas 0-6
    
    **Serviços Presenciais Mais Vendidos** – Ranking de serviços presenciais por receita e quantidade.
    
    **Vouchers Utilizados** – Vouchers do ecommerce que foram efetivamente utilizados na unidade (filtrados por `USED_DATE` e `AFILLIATION_NAME`).
    
    **Vouchers Omnichannel** – Todos os vouchers vendidos para a unidade, independente se foram utilizados ou não (filtrados por `CREATED_DATE`).
    
    **Distribuição de Receita** – Divisão da receita entre Belle (Sistema Local), Ecommerce (Vouchers) e Parcerias Comerciais (TotalPass, GymPass, ClassPass).
    
    ### 🎫 Sobre Vouchers
    
    **Vouchers Utilizados:** Mostram apenas os vouchers que já foram utilizados na sua unidade (data: USED_DATE).
    
    **Vouchers Omnichannel:** Mostram todos os vouchers vendidos para sua unidade, independente se foram utilizados ou não (data: CREATED_DATE).
    
    ### 💡 Dicas de Uso
    
    - Use os **ícones ℹ️** ao lado das métricas para ver explicações detalhadas
    - Na aba **Self-Service**, você pode criar análises personalizadas
    - Na aba **Financeiro**, veja o detalhamento completo por origem (Belle, Ecommerce, Parcerias)
    """)
     
    st.caption("Buddha Spa Dashboard – Portal de Franqueados v3.1 FINAL CORRIGIDO")
