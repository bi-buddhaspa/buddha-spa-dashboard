# =====================================================================
# BUDDHA SPA - DASHBOARD DE FRANQUEADOS V3.0
# Código completo e pronto para uso
# =====================================================================

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

st.set_page_config(
    page_title="Buddha Spa Analytics",
    page_icon="🪷",
    layout="wide"
)

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

st.markdown("""
    <style>
        .stMetric {background-color: #F5F0E6; padding: 15px; border-radius: 10px; border: 2px solid #8B0000;}
        .stMetric label {color: #8B0000 !important; font-weight: bold; font-size: 0.9rem;}
        .stMetric [data-testid="stMetricValue"] {color: #2C1810; font-size: 1.5rem !important; font-weight: 600;}
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
    SELECT id_venda, unidade, DATE(data_atendimento) AS data_atendimento, nome_cliente, profissional,
           forma_pagamento, nome_servico_simplificado, SUM(valor_liquido) AS valor_liquido,
           SUM(valor_bruto) AS valor_bruto, COUNT(*) AS qtd_itens
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
    SELECT id_venda, unidade, DATE(data_atendimento) AS data_atendimento, nome_cliente, profissional,
           forma_pagamento, nome_servico_simplificado, valor_liquido, valor_bruto
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
    SELECT s.ID, s.NAME, s.STATUS, s.COUPONS, s.CREATED_DATE,
           DATETIME(s.CREATED_DATE, "America/Sao_Paulo") AS CREATED_DATE_BRAZIL,
           s.USED_DATE, DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL,
           s.PRICE_NET, s.PRICE_GROSS, s.PRICE_REFOUND, s.KEY, s.ORDER_ID,
           (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
           u.post_title AS AFILLIATION_NAME,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_city' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_City,
           (SELECT MAX(CASE WHEN pm.meta_key = '_billing_state' THEN pm.meta_value END) 
            FROM `buddha-bigdata.raw.wp_posts` o LEFT JOIN `buddha-bigdata.raw.wp_postmeta` pm ON o.ID = pm.post_id
            WHERE o.ID = CAST(CAST(s.ORDER_ID AS FLOAT64) AS INT64)) AS Customer_State
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
    SELECT DATE(data) AS data, unidade, classificacao_padronizada, nota,
           flag_promotor, flag_neutro, flag_detrator
    FROM `buddha-bigdata.analytics.analise_nps_analytics`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}') {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_instagram_posts(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT DATE(data) AS data_post, nome, visualizacoes, compartilhamentos, curtidas,
           comentarios, impressoes, alcance, vendas, id_post
    FROM `buddha-bigdata.raw.instagram_posts`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_instagram_seguidores(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT DATE(data) AS data_registro, qtd_seguidores
    FROM `buddha-bigdata.raw.instagram_seguidores`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
    ORDER BY data_registro
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_meta_ads(data_inicio, data_fim):
    client = get_bigquery_client()
    query = f"""
    SELECT DATE(data) AS data, nome, impressoes, alcance, cliques, vendas, investido, vendas_valor
    FROM `buddha-bigdata.raw.meta_ads`
    WHERE DATE(data) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
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
        unidades_disponiveis = load_unidades()
        unidades_selecionadas = st.sidebar.multiselect("Unidades:", options=unidades_disponiveis, default=None)
    except Exception as e:
        st.error(f"Erro ao carregar unidades: {e}")
        st.stop()
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

with st.spinner("Carregando dados de vouchers..."):
    try:
        unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
        df_ecom_calc = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
    except Exception as e:
        st.error(f"Erro ao carregar dados de vouchers: {e}")
        df_ecom_calc = pd.DataFrame()

data_col = 'data_atendimento'
valor_col = 'valor_liquido'

col_logo, col_title = st.columns([1, 5])
with col_logo:
    st.image("https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTkFQZ7QiSmOpEWC_9Ndsuqx_-roUMRJJkCvw&s", width=200)
with col_title:
    st.title("Buddha Spa - Dashboard de Unidades")
    st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

receita_belle = df[valor_col].sum()
qtd_atendimentos = int(df['id_venda'].nunique())
qtd_clientes = int(df['nome_cliente'].nunique()) if 'nome_cliente' in df.columns else 0

receita_ecommerce = 0
receita_parcerias = 0
if not df_ecom_calc.empty:
    df_ecom_calc['PRICE_NET'] = pd.to_numeric(df_ecom_calc['PRICE_NET'], errors='coerce')
    df_ecom_sem_cupom = df_ecom_calc[(df_ecom_calc['COUPONS'].isna()) | (df_ecom_calc['COUPONS'] == '')]
    df_ecom_com_cupom = df_ecom_calc[(df_ecom_calc['COUPONS'].notna()) & (df_ecom_calc['COUPONS'] != '')]
    receita_ecommerce = df_ecom_sem_cupom['PRICE_NET'].fillna(0).sum()
    receita_parcerias = df_ecom_com_cupom['PRICE_NET'].fillna(0).sum()

faturamento_total = receita_belle + receita_ecommerce + receita_parcerias
df_com_receita = df[df[valor_col] > 0]
qtd_atendimentos_pagos = int(df_com_receita['id_venda'].nunique())
ticket_medio = receita_belle / qtd_atendimentos_pagos if qtd_atendimentos_pagos > 0 else 0

colk1, colk2, colk3, colk4 = st.columns(4)

with colk1:
    st.metric("Faturamento Total", formatar_moeda(faturamento_total))
    with st.popover("ℹ️"):
        st.markdown(f"""
        **FATURAMENTO TOTAL = {formatar_moeda(faturamento_total)}**
        
        Este é TODO o dinheiro que entrou na sua unidade no período, somando 3 fontes:
        
        **1. Vendas Presenciais (Belle): {formatar_moeda(receita_belle)}**
        - Atendimentos pagos diretamente na unidade
        - Formas de pagamento: Cartão, Dinheiro, PIX, etc.
        - Registrado no sistema Belle
        
        **2. Vouchers do Site: {formatar_moeda(receita_ecommerce)}**
        - Vouchers comprados no site buddhaspa.com.br
        - Cliente usou o voucher na sua unidade
        - Sem cupons de desconto de parceiros
        
        **3. Parcerias (com cupons): {formatar_moeda(receita_parcerias)}**
        - Vouchers com cupons de parceiros
        - Exemplos: convênios empresas, promoções especiais
        
        **Período:** {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
        
        💡 **Por que importa:** Mostra toda a receita da unidade, não importa de onde veio o pagamento.
        """)

with colk2:
    st.metric("Quantidade de Atendimentos", formatar_numero(qtd_atendimentos))
    with st.popover("ℹ️"):
        st.markdown(f"""
        **QUANTIDADE DE ATENDIMENTOS = {formatar_numero(qtd_atendimentos)}**
        
        Total de atendimentos realizados na unidade.
        
        **Como contamos:**
        - Cada atendimento = 1 vez que você atendeu um cliente
        - Se o mesmo cliente voltou = conta de novo
        - Exemplo: Maria fez 3 massagens = 3 atendimentos
        
        **O que conta:**
        ✅ Todos os serviços realizados (massagem, estética, etc.)
        ✅ Atendimentos pagos e gratuitos
        ✅ Cada sessão conta separadamente
        
        **O que NÃO conta:**
        ❌ Venda de produtos
        ❌ Atendimentos cancelados
        
        **Período:** {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
        
        💡 **Por que importa:** Mostra o volume de trabalho da unidade e quanto a equipe está ocupada.
        """)

with colk3:
    st.metric("Clientes Únicos", formatar_numero(qtd_clientes))
    with st.popover("ℹ️"):
        st.markdown(f"""
        **CLIENTES ÚNICOS = {formatar_numero(qtd_clientes)}**
        
        Quantos clientes DIFERENTES foram atendidos.
        
        **Como contamos:**
        - Cada cliente conta apenas 1 vez
        - Mesmo que ele tenha voltado várias vezes
        
        **Exemplo:**
        - Maria fez 3 massagens no mês
        - João fez 2 massagens no mês
        - Total de atendimentos: 5
        - Clientes únicos: 2 (Maria e João)
        
        **Na sua unidade:**
        - Total de atendimentos: {formatar_numero(qtd_atendimentos)}
        - Clientes únicos: {formatar_numero(qtd_clientes)}
        - Cada cliente veio em média: {(qtd_atendimentos / qtd_clientes):.1f} vezes
        
        💡 **Por que importa:** Mostra se você tem clientes fiéis (que voltam) ou se precisa atrair mais clientes novos.
        """)

with colk4:
    st.metric("Ticket Médio por Atendimento", formatar_moeda(ticket_medio))
    with st.popover("ℹ️"):
        st.markdown(f"""
        **TICKET MÉDIO = {formatar_moeda(ticket_medio)}**
        
        Quanto cada atendimento gerou de receita, em média.
        
        **Como calculamos:**
        - Pegamos toda a receita das vendas presenciais: {formatar_moeda(receita_belle)}
        - Dividimos pelos atendimentos pagos: {formatar_numero(qtd_atendimentos_pagos)}
        - Resultado: {formatar_moeda(receita_belle)} ÷ {formatar_numero(qtd_atendimentos_pagos)} = {formatar_moeda(ticket_medio)}
        
        **Atenção:** Só contamos atendimentos que geraram receita (não conta cortesias)
        
        **O que aumenta o ticket médio:**
        ⬆️ Vender serviços mais caros (ex: Day Spa)
        ⬆️ Cliente comprar combo de serviços
        ⬆️ Menos descontos
        
        **O que diminui:**
        ⬇️ Serviços mais baratos
        ⬇️ Muitas promoções
        
        💡 **Por que importa:** Mostra se você está vendendo bem ou precisa trabalhar para aumentar o valor das vendas.
        """)

if is_admin and unidades_selecionadas:
    st.markdown("---")
    st.info(f"**📍 Unidades selecionadas:** {', '.join([u.title() for u in unidades_selecionadas])}")
elif not is_admin:
    st.markdown("---")
    st.info(f"**📍 Visualizando unidade:** {unidade_usuario.title()}")

st.divider()

tab_visao, tab_atend, tab_fin, tab_mkt, tab_selfservice, tab_gloss = st.tabs(
    ["Visão Geral", "Atendimento", "Financeiro", "Marketing & Ecommerce", "Self-Service", "Ajuda / Glossário"]
)

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
        fig = px.line(df_evolucao_completo, x=data_col, y=valor_col, color='unidade', markers=True,
                      labels={valor_col: 'Receita (R$)', data_col: 'Data', 'unidade': 'Unidade'})
        for trace in fig.data:
            if trace.name == 'Média da Rede':
                trace.line.dash = 'dash'
                trace.line.width = 3
                trace.line.color = '#FF6B6B'
    else:
        df_evolucao = df.groupby(data_col)[valor_col].sum().reset_index().sort_values(data_col)
        df_evolucao['unidade'] = unidade_usuario.title() if not is_admin else 'Unidade Selecionada'
        df_media_rede = df_todas_unidades.groupby([data_col, 'unidade'])[valor_col].sum().reset_index().groupby(data_col)[valor_col].mean().reset_index()
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
    
    fig.update_layout(xaxis_title="Data", yaxis_title="Receita (R$)", height=400, plot_bgcolor='#FFFFFF',
                      paper_bgcolor='#F5F0E6', legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                      xaxis=dict(tickformat='%d/%m', tickmode='auto', nticks=15, showgrid=True, gridcolor='lightgray'),
                      yaxis=dict(showgrid=True, gridcolor='lightgray'))
    fig.update_yaxes(tickformat=",.2f")
    st.plotly_chart(fig, use_container_width=True)
    
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
        with col_nps1:
            st.metric("NPS Score", formatar_percentual(nps_score))
        with col_nps2:
            st.metric("Promotores", f"{promotores} ({formatar_percentual(perc_promotores)})")
        with col_nps3:
            st.metric("Neutros", f"{neutros} ({formatar_percentual(perc_neutros)})")
        with col_nps4:
            st.metric("Detratores", f"{detratores} ({formatar_percentual(perc_detratores)})")
        
        df_nps_dist = pd.DataFrame({
            'Classificação': ['Promotores', 'Neutros', 'Detratores'],
            'Quantidade': [promotores, neutros, detratores]
        })
        fig_nps = px.pie(df_nps_dist, names='Classificação', values='Quantidade', color='Classificação',
                         color_discrete_map={'Promotores': '#2E7D32', 'Neutros': '#FFA726', 'Detratores': '#D32F2F'})
        fig_nps.update_traces(textposition='inside', textinfo='percent')
        fig_nps.update_layout(paper_bgcolor='#F5F0E6', height=400, showlegend=True)
        st.plotly_chart(fig_nps, use_container_width=True)
    else:
        st.info("Sem dados de NPS para o período selecionado.")

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
        
        # TOTALIZADOR
        total_row = pd.DataFrame({
            'unidade': ['TOTAL'],
            'profissional': ['TODOS OS TERAPEUTAS'],
            'receita': [df_terap['receita'].sum()],
            'qtd_atendimentos': [df_terap['qtd_atendimentos'].sum()],
            'clientes_unicos': [df_terap['clientes_unicos'].sum()],
            'ticket_medio': [df_terap['receita'].sum() / df_terap['qtd_atendimentos'].sum() if df_terap['qtd_atendimentos'].sum() > 0 else 0]
        })
        df_terap_com_total = pd.concat([df_terap, total_row], ignore_index=True)
        df_terap_display = df_terap_com_total.copy()
        df_terap_display['receita'] = df_terap_display['receita'].apply(formatar_moeda)
        df_terap_display['qtd_atendimentos'] = df_terap_display['qtd_atendimentos'].apply(formatar_numero)
        df_terap_display['clientes_unicos'] = df_terap_display['clientes_unicos'].apply(formatar_numero)
        df_terap_display['ticket_medio'] = df_terap_display['ticket_medio'].apply(formatar_moeda)
        st.dataframe(df_terap_display, use_container_width=True, height=500)
    
    st.markdown("---")
    st.subheader("Atendimentos por Dia da Semana")
    df_dia_semana = df_detalhado.copy()
    df_dia_semana['dia_semana'] = pd.to_datetime(df_dia_semana[data_col]).dt.day_name()
    dias_map = {'Monday': 'Segunda', 'Tuesday': 'Terça', 'Wednesday': 'Quarta', 'Thursday': 'Quinta',
                'Friday': 'Sexta', 'Saturday': 'Sábado', 'Sunday': 'Domingo'}
    df_dia_semana['dia_semana'] = df_dia_semana['dia_semana'].map(dias_map)
    df_agrupado = df_dia_semana.groupby('dia_semana').size().reset_index(name='qtd')
    dias_ordem = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    df_agrupado['dia_semana'] = pd.Categorical(df_agrupado['dia_semana'], categories=dias_ordem, ordered=True)
    df_agrupado = df_agrupado.sort_values('dia_semana')
    fig_dia = px.bar(df_agrupado, x='dia_semana', y='qtd', text='qtd', color_discrete_sequence=['#8B0000'])
    fig_dia.update_traces(textposition='outside')
    fig_dia.update_layout(plot_bgcolor='#FFFFFF', paper_bgcolor='#F5F0E6', height=400)
    st.plotly_chart(fig_dia, use_container_width=True)

with tab_fin:
    st.subheader("Faturamento por Origem")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("💳 Vendas Belle (Presencial)", formatar_moeda(receita_belle))
    with col2:
        st.metric("🎫 Vouchers Ecommerce", formatar_moeda(receita_ecommerce))
    with col3:
        st.metric("🤝 Parcerias (Cupons)", formatar_moeda(receita_parcerias))
    
    st.metric("💰 FATURAMENTO TOTAL", formatar_moeda(faturamento_total))
    
    df_dist = pd.DataFrame({
        'Origem': ['Belle', 'Ecommerce', 'Parcerias'],
        'Receita': [receita_belle, receita_ecommerce, receita_parcerias]
    })
    df_dist = df_dist[df_dist['Receita'] > 0]
    fig_dist = px.pie(df_dist, names='Origem', values='Receita')
    fig_dist.update_layout(paper_bgcolor='#F5F0E6', height=400)
    st.plotly_chart(fig_dist, use_container_width=True)

with tab_selfservice:
    st.subheader("Monte Sua Própria Análise")
    c1, c2 = st.columns(2)
    with c1:
        dimensoes = st.multiselect("Agrupar Por:", ["Data", "Unidade", "Forma de Pagamento", "Serviço", "Terapeuta", "Cliente"], default=["Unidade"])
    with c2:
        metricas = st.multiselect("Métricas:", ["Receita Total", "Quantidade de Atendimentos", "Ticket Médio", "Clientes Únicos"], default=["Receita Total"])
    
    if dimensoes and metricas:
        dim_map = {"Data": data_col, "Unidade": "unidade", "Forma de Pagamento": "forma_pagamento",
                   "Serviço": "nome_servico_simplificado", "Terapeuta": "profissional", "Cliente": "nome_cliente"}
        colunas = [dim_map[d] for d in dimensoes if dim_map[d] in df.columns]
        
        if colunas:
            agg_dict = {}
            if "Receita Total" in metricas:
                agg_dict['receita_total'] = (valor_col, 'sum')
            if "Quantidade de Atendimentos" in metricas:
                agg_dict['qtd_atendimentos'] = ('id_venda', 'nunique')
            if "Clientes Únicos" in metricas and 'nome_cliente' in df.columns:
                agg_dict['clientes_unicos'] = ('nome_cliente', 'nunique')
            
            df_custom = df.groupby(colunas).agg(**agg_dict).reset_index()
            if "Ticket Médio" in metricas and 'receita_total' in df_custom.columns and 'qtd_atendimentos' in df_custom.columns:
                df_custom['ticket_medio'] = df_custom['receita_total'] / df_custom['qtd_atendimentos']
            
            # TOTALIZADOR
            total_dict = {col: 'TOTAL' if i == 0 else '' for i, col in enumerate(colunas)}
            for col in df_custom.columns:
                if col not in colunas:
                    total_dict[col] = df_custom[col].sum() if col != 'ticket_medio' else (df_custom['receita_total'].sum() / df_custom['qtd_atendimentos'].sum() if 'qtd_atendimentos' in df_custom.columns else 0)
            total_row = pd.DataFrame([total_dict])
            df_custom_com_total = pd.concat([df_custom, total_row], ignore_index=True)
            
            df_display = df_custom_com_total.copy()
            if data_col in df_display.columns:
                df_display[data_col] = pd.to_datetime(df_display[data_col], errors='coerce').dt.strftime('%d/%m/%Y')
            if 'receita_total' in df_display.columns:
                df_display['receita_total'] = df_display['receita_total'].apply(formatar_moeda)
            if 'ticket_medio' in df_display.columns:
                df_display['ticket_medio'] = df_display['ticket_medio'].apply(formatar_moeda)
            if 'qtd_atendimentos' in df_display.columns:
                df_display['qtd_atendimentos'] = df_display['qtd_atendimentos'].apply(formatar_numero)
            if 'clientes_unicos' in df_display.columns:
                df_display['clientes_unicos'] = df_display['clientes_unicos'].apply(formatar_numero)
            
            st.dataframe(df_display, use_container_width=True, height=400)
            
            csv = df_custom_com_total.to_csv(index=False, encoding='utf-8-sig').encode('utf-8-sig')
            st.download_button("📥 Download CSV", csv, f"buddha_selfservice_{data_inicio.strftime('%d%m%Y')}_{data_fim.strftime('%d%m%Y')}.csv", "text/csv")

with tab_gloss:
    st.subheader("Ajuda / Glossário")
    st.markdown("""
    ### 📊 Principais Métricas
    
    **Faturamento Total** – TODO o dinheiro que entrou na unidade (Belle + Ecommerce + Parcerias)
    
    **Vendas Belle** – Atendimentos pagos presencialmente (cartão, dinheiro, PIX)
    
    **Vouchers Ecommerce** – Vouchers do site usados na unidade (sem cupons)
    
    **Parcerias** – Vouchers com cupons de parceiros
    
    **Quantidade de Atendimentos** – Total de atendimentos realizados
    
    **Clientes Únicos** – Quantos clientes diferentes foram atendidos
    
    **Ticket Médio** – Valor médio por atendimento (Receita ÷ Atendimentos)
    
    **NPS** – Satisfação do cliente: % Promotores - % Detratores
    """)
