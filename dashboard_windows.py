import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import locale

# Configurar localização brasileira
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except:
        pass

# ========================================
# CONFIGURAÇÃO DA PÁGINA
# ========================================

st.set_page_config(
    page_title="Buddha Spa - Dashboard",
    page_icon="🧘",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========================================
# AUTENTICAÇÃO
# ========================================

def check_password():
    """Retorna `True` se o usuário tiver a senha correta."""
    
    def password_entered():
        """Verifica se a senha digitada está correta."""
        users = {
            "admin": "buddha2024",
            "usuario": "spa123"
        }
        
        if st.session_state["username"] in users and \
           st.session_state["password"] == users[st.session_state["username"]]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]
            del st.session_state["username"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.title("🧘 Buddha Spa - Dashboard")
        st.text_input("Usuário", key="username")
        st.text_input("Senha", type="password", key="password")
        st.button("Login", on_click=password_entered)
        return False
    elif not st.session_state["password_correct"]:
        st.title("🧘 Buddha Spa - Dashboard")
        st.text_input("Usuário", key="username")
        st.text_input("Senha", type="password", key="password")
        st.button("Login", on_click=password_entered)
        st.error("😕 Usuário ou senha incorretos")
        return False
    else:
        return True

if not check_password():
    st.stop()

# ========================================
# CONEXÃO BIGQUERY
# ========================================

@st.cache_resource
def get_bigquery_client():
    """Cria conexão com BigQuery usando credenciais do Streamlit secrets."""
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials, project=st.secrets["gcp_service_account"]["project_id"])

# ========================================
# FUNÇÕES DE FORMATAÇÃO
# ========================================

def formatar_moeda(valor):
    """Formata valor como moeda brasileira."""
    try:
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except:
        return "R$ 0,00"

def formatar_numero(valor):
    """Formata número com separador de milhares."""
    try:
        return f"{valor:,.0f}".replace(",", ".")
    except:
        return "0"

def formatar_percentual(valor):
    """Formata valor como percentual."""
    try:
        return f"{valor:.1f}%".replace(".", ",")
    except:
        return "0,0%"

# ========================================
# UPLOAD DE PLANILHAS (TEMPORÁRIO)
# ========================================

with st.sidebar.expander("🔧 Upload de Dados (Admin)"):
    st.write("**Gestão de Planilhas**")
    
    # Upload Participações em Eventos
    st.subheader("📅 Participações em Eventos")
    uploaded_eventos = st.file_uploader(
        "Planilha de Eventos",
        type=['xlsx', 'xls'],
        key='upload_eventos'
    )
    
    if uploaded_eventos and st.button("📤 Upload Eventos", key="btn_eventos"):
        with st.spinner("Processando eventos..."):
            try:
                # Ler Excel
                df = pd.read_excel(uploaded_eventos)
                df = df.dropna(how='all')
                
                # Renomear colunas
                colunas_map = {
                    'ID_Belle': 'id_belle',
                    'Franqueado': 'franqueado',
                    'Unidade': 'unidade',
                    'Ação': 'acao',
                    'Complemento da Ação': 'complemento_acao',
                    'Data': 'data_evento',
                    'Presença': 'presenca'
                }
                
                for old_col, new_col in colunas_map.items():
                    if old_col in df.columns:
                        df = df.rename(columns={old_col: new_col})
                
                # Converter tipos
                if 'id_belle' in df.columns:
                    df['id_belle'] = pd.to_numeric(df['id_belle'], errors='coerce').astype('Int64')
                
                if 'data_evento' in df.columns:
                    df['data_evento'] = pd.to_datetime(df['data_evento'], format='%d/%m/%Y', errors='coerce')
                
                if 'presenca' in df.columns:
                    df['presenca'] = pd.to_numeric(df['presenca'], errors='coerce').fillna(0).astype(int)
                
                # Limpar textos
                for col in ['franqueado', 'unidade', 'acao', 'complemento_acao']:
                    if col in df.columns:
                        df[col] = (df[col].astype(str)
                                  .str.replace('\n', ' ')
                                  .str.replace('\r', ' ')
                                  .str.replace('\t', ' ')
                                  .str.strip()
                                  .replace('nan', ''))
                
                # Upload para BigQuery
                client = get_bigquery_client()
                table_id = "buddha-bigdata.raw.participacoes_eventos_raw"
                
                schema = [
                    bigquery.SchemaField("id_belle", "INTEGER"),
                    bigquery.SchemaField("franqueado", "STRING"),
                    bigquery.SchemaField("unidade", "STRING"),
                    bigquery.SchemaField("acao", "STRING"),
                    bigquery.SchemaField("complemento_acao", "STRING"),
                    bigquery.SchemaField("data_evento", "DATE"),
                    bigquery.SchemaField("presenca", "INTEGER"),
                ]
                
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition="WRITE_TRUNCATE",
                )
                
                job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()
                
                st.success(f"✅ {job.output_rows} eventos enviados!")
                
            except Exception as e:
                st.error(f"❌ Erro: {str(e)}")
    
    st.divider()
    
    # Upload Reclame Aqui
    st.subheader("😠 Reclame Aqui")
    uploaded_reclame = st.file_uploader(
        "Planilha de Reclamações",
        type=['xlsx', 'xls'],
        key='upload_reclame'
    )
    
    if uploaded_reclame and st.button("📤 Upload Reclame Aqui", key="btn_reclame"):
        with st.spinner("Processando reclamações..."):
            try:
                # Ler Excel
                df = pd.read_excel(uploaded_reclame)
                df = df.dropna(how='all')
                
                # Renomear colunas
                colunas_map = {
                    'Exercício': 'exercicio',
                    'UNIDADES': 'unidade',
                    'RECLAMAÇÃO': 'reclamacao',
                    'TOTAL DE CLIENTES': 'total_clientes',
                    'RECLAMAÇÃO X CLIENTES': 'perc_reclamacao',
                    'NOTA': 'nota'
                }
                
                for old_col, new_col in colunas_map.items():
                    if old_col in df.columns:
                        df = df.rename(columns={old_col: new_col})
                
                # Converter tipos
                if 'exercicio' in df.columns:
                    df['data_exercicio'] = pd.to_datetime(df['exercicio'], format='%d/%m/%Y', errors='coerce')
                
                if 'reclamacao' in df.columns:
                    df['reclamacao'] = pd.to_numeric(df['reclamacao'], errors='coerce').fillna(0).astype(int)
                
                if 'total_clientes' in df.columns:
                    df['total_clientes'] = pd.to_numeric(df['total_clientes'], errors='coerce').fillna(0).astype(int)
                
                if 'perc_reclamacao' in df.columns:
                    df['perc_reclamacao'] = (df['perc_reclamacao'].astype(str)
                                             .str.replace('%', '')
                                             .str.replace(',', '.')
                                             .pipe(pd.to_numeric, errors='coerce'))
                
                if 'nota' in df.columns:
                    df['nota'] = pd.to_numeric(df['nota'], errors='coerce').fillna(0).astype(int)
                
                if 'unidade' in df.columns:
                    df['unidade'] = df['unidade'].astype(str).str.strip().str.lower()
                
                # Upload para BigQuery
                client = get_bigquery_client()
                table_id = "buddha-bigdata.raw.reclame_aqui_raw"
                
                schema = [
                    bigquery.SchemaField("data_exercicio", "DATE"),
                    bigquery.SchemaField("unidade", "STRING"),
                    bigquery.SchemaField("reclamacao", "INTEGER"),
                    bigquery.SchemaField("total_clientes", "INTEGER"),
                    bigquery.SchemaField("perc_reclamacao", "FLOAT"),
                    bigquery.SchemaField("nota", "INTEGER"),
                ]
                
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition="WRITE_TRUNCATE",
                )
                
                job = client.load_table_from_dataframe(
                    df[['data_exercicio', 'unidade', 'reclamacao', 'total_clientes', 'perc_reclamacao', 'nota']], 
                    table_id, 
                    job_config=job_config
                )
                job.result()
                
                st.success(f"✅ {job.output_rows} reclamações enviadas!")
                
            except Exception as e:
                st.error(f"❌ Erro: {str(e)}")

# ========================================
# FUNÇÕES DE CARREGAMENTO DE DADOS
# ========================================

@st.cache_data(ttl=3600)
def load_omnichannel_vouchers(data_inicio, data_fim, unidades_filtro=None):
    """Carrega vouchers omnichannel (todos vendidos, não apenas usados)"""
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
        s.KEY, 
        s.ORDER_ID,
        (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
        CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) AS AFILLIATION_ID,
        u.post_title AS AFILLIATION_NAME
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
def load_ecommerce(data_inicio, data_fim, unidades_filtro=None):
    """Carrega dados de ecommerce (vouchers utilizados)"""
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
    SELECT 
        s.ID, 
        s.NAME, 
        s.STATUS, 
        s.COUPONS, 
        s.USED_DATE,
        DATETIME(s.USED_DATE, "America/Sao_Paulo") AS USED_DATE_BRAZIL,
        s.PRICE_NET, 
        s.PRICE_GROSS, 
        s.KEY, 
        s.ORDER_ID,
        (SELECT p.NAME FROM `buddha-bigdata.raw.packages_raw` p WHERE p.ID = s.PACKAGE_ID) AS PACKAGE_NAME,
        CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64) AS AFILLIATION_ID,
        u.post_title AS AFILLIATION_NAME
    FROM `buddha-bigdata.raw.ecommerce_raw` s
    INNER JOIN `buddha-bigdata.raw.wp_posts` u 
        ON u.post_type = 'unidade' 
        AND u.ID = CAST(CAST(s.AFILLIATION_ID AS FLOAT64) AS INT64)
        AND s.NAME LIKE CONCAT('% - ', u.post_title, '%')
    WHERE 
        DATE(DATETIME(s.USED_DATE, "America/Sao_Paulo")) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
        AND (s.STATUS = '2' OR s.STATUS = '3')
        AND s.AFILLIATION_ID IS NOT NULL
        AND s.NAME NOT LIKE '%Voucher Experience%'
        {filtro_unidade}
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_unidades():
    """Carrega lista de unidades"""
    client = get_bigquery_client()
    query = """
    SELECT DISTINCT 
        LOWER(CONCAT('buddha spa - ', post_title)) as unidade
    FROM `buddha-bigdata.raw.wp_posts` 
    WHERE post_type = 'unidade'
    AND post_status = 'publish'
    ORDER BY post_title
    """
    df = client.query(query).to_dataframe()
    return df['unidade'].tolist()

# ========================================
# INTERFACE PRINCIPAL
# ========================================

st.title("🧘 Buddha Spa - Dashboard Franquias")

# Sidebar - Filtros
st.sidebar.header("Filtros")

# Filtro de Data
data_inicio = st.sidebar.date_input(
    "Data Início",
    value=datetime.now() - timedelta(days=30),
    max_value=datetime.now()
)

data_fim = st.sidebar.date_input(
    "Data Fim",
    value=datetime.now(),
    max_value=datetime.now()
)

# Filtro de Unidades
try:
    unidades_disponiveis = load_unidades()
except:
    unidades_disponiveis = []

unidades_selecionadas = st.sidebar.multiselect(
    "Unidades",
    options=unidades_disponiveis,
    default=[]
)

# ========================================
# ABAS PRINCIPAIS
# ========================================

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📊 Visão Geral",
    "📞 Atendimento",
    "💰 Financeiro",
    "🛍️ Marketing & Ecommerce",
    "🤖 Self-Service",
    "📞 Suporte",
    "📖 Glossário"
])

# ========================================
# ABA: VISÃO GERAL
# ========================================

with tab1:
    st.header("📊 Visão Geral")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Período Selecionado", f"{(data_fim - data_inicio).days} dias")
    
    with col2:
        st.metric("Unidades Filtradas", len(unidades_selecionadas) if unidades_selecionadas else "Todas")
    
    with col3:
        st.metric("Total Receita", formatar_moeda(0))
    
    with col4:
        st.metric("Total Atendimentos", formatar_numero(0))
    
    st.info("📊 Dashboard em desenvolvimento. Mais métricas serão adicionadas em breve.")

# ========================================
# ABA: ATENDIMENTO
# ========================================

with tab2:
    st.header("📞 Atendimento")
    st.info("🚧 Seção em desenvolvimento")

# ========================================
# ABA: FINANCEIRO
# ========================================

with tab3:
    st.header("💰 Financeiro")
    st.info("🚧 Seção em desenvolvimento")

# ========================================
# ABA: MARKETING & ECOMMERCE
# ========================================

with tab4:
    st.header("🛍️ Marketing & Ecommerce")
    
    # Seção: Vouchers Utilizados
    st.subheader("🎫 Vouchers Utilizados")
    
    try:
        df_ecom = load_ecommerce(
            data_inicio.strftime('%Y-%m-%d'),
            data_fim.strftime('%Y-%m-%d'),
            unidades_selecionadas
        )
        
        if not df_ecom.empty:
            df_ecom['PRICE_NET'] = pd.to_numeric(df_ecom['PRICE_NET'], errors='coerce').fillna(0)
            df_ecom['PRICE_GROSS'] = pd.to_numeric(df_ecom['PRICE_GROSS'], errors='coerce').fillna(0)
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Vouchers Utilizados", formatar_numero(len(df_ecom)))
            
            with col2:
                st.metric("Receita Bruta", formatar_moeda(df_ecom['PRICE_GROSS'].sum()))
            
            with col3:
                st.metric("Receita Líquida", formatar_moeda(df_ecom['PRICE_NET'].sum()))
            
            with col4:
                desconto_total = df_ecom['PRICE_GROSS'].sum() - df_ecom['PRICE_NET'].sum()
                st.metric("Desconto Total", formatar_moeda(desconto_total))
            
            # Gráfico de vouchers por unidade
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📊 Top 10 Unidades")
                unidades_receita = (df_ecom.groupby('AFILLIATION_NAME')['PRICE_NET']
                                    .sum()
                                    .sort_values(ascending=False)
                                    .head(10))
                
                fig = px.bar(
                    x=unidades_receita.values,
                    y=unidades_receita.index,
                    orientation='h',
                    title='Receita por Unidade',
                    labels={'x': 'Receita (R$)', 'y': 'Unidade'}
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("📊 Performance de Cupons")
                df_com_cupom = df_ecom[df_ecom['COUPONS'].notna() & (df_ecom['COUPONS'] != '')]
                
                if not df_com_cupom.empty:
                    col_a, col_b, col_c = st.columns(3)
                    
                    with col_a:
                        vouchers_com_cupom = len(df_com_cupom)
                        perc_cupom = (vouchers_com_cupom / len(df_ecom)) * 100
                        st.metric("Vouchers com Cupom", f"{vouchers_com_cupom} ({perc_cupom:.1f}%)")
                    
                    with col_b:
                        receita_com_cupom = df_com_cupom['PRICE_NET'].sum()
                        st.metric("Receita com Cupons", formatar_moeda(receita_com_cupom))
                    
                    with col_c:
                        desconto_cupons = df_com_cupom['PRICE_GROSS'].sum() - df_com_cupom['PRICE_NET'].sum()
                        st.metric("Desconto Total Aplicado", formatar_moeda(desconto_cupons))
                else:
                    st.info("Nenhum cupom utilizado no período")
        
        else:
            st.warning("Sem dados de vouchers utilizados para o período selecionado")
    
    except Exception as e:
        st.error(f"Erro ao carregar vouchers utilizados: {str(e)}")
    
    st.divider()
    
    # ========================================
    # SEÇÃO: VOUCHERS OMNICHANNEL
    # ========================================
    
    st.subheader("🎫 Vouchers Omnichannel - Todos os Vouchers Vendidos")
    
    with st.expander("ℹ️ Sobre esta seção"):
        st.write("""
        **Diferença para a seção anterior:**
        - **Seção anterior**: Mostra apenas vouchers **utilizados** (filtrados por USED_DATE)
        - **Esta seção**: Mostra **todos os vouchers vendidos** (filtrados por CREATED_DATE)
        
        **Fonte dos dados**: Data de criação do voucher (CREATED_DATE)
        
        **Legenda de Status:**
        - **Status 1**: Disponível (voucher ainda não foi usado)
        - **Status 2**: Utilizado
        - **Status 3**: Utilizado
        """)
    
    try:
        df_omni = load_omnichannel_vouchers(
            data_inicio.strftime('%Y-%m-%d'),
            data_fim.strftime('%Y-%m-%d'),
            unidades_selecionadas
        )
        
        if not df_omni.empty:
            df_omni['PRICE_NET'] = pd.to_numeric(df_omni['PRICE_NET'], errors='coerce').fillna(0)
            df_omni['PRICE_GROSS'] = pd.to_numeric(df_omni['PRICE_GROSS'], errors='coerce').fillna(0)
            
            # KPIs
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total Vouchers Vendidos", formatar_numero(len(df_omni)))
            
            with col2:
                st.metric("Total Pedidos", formatar_numero(df_omni['ORDER_ID'].nunique()))
            
            with col3:
                st.metric("Receita Bruta", formatar_moeda(df_omni['PRICE_GROSS'].sum()))
            
            with col4:
                st.metric("Receita Líquida", formatar_moeda(df_omni['PRICE_NET'].sum()))
            
            # Gráficos
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("📊 Vouchers por Status")
                
                status_counts = df_omni['STATUS'].value_counts()
                
                status_labels = {
                    '1': 'Disponível (1)',
                    '2': 'Utilizado (2)',
                    '3': 'Utilizado (3)'
                }
                
                status_df = pd.DataFrame({
                    'Status': [status_labels.get(str(k), f'Status {k}') for k in status_counts.index],
                    'Quantidade': status_counts.values
                })
                
                fig_status = px.pie(
                    status_df,
                    values='Quantidade',
                    names='Status',
                    title='Distribuição por Status',
                    color_discrete_sequence=px.colors.sequential.RdBu
                )
                st.plotly_chart(fig_status, use_container_width=True)
                
                st.caption("**Legenda:** Status 1 = Disponível | Status 2 e 3 = Utilizado")
            
            with col2:
                st.subheader("🏢 Top 10 Unidades por Receita")
                
                unidades_receita = (df_omni.groupby('AFILLIATION_NAME')['PRICE_NET']
                                    .sum()
                                    .sort_values(ascending=False)
                                    .head(10))
                
                fig_unidades = px.bar(
                    x=unidades_receita.values,
                    y=unidades_receita.index,
                    orientation='h',
                    title='Receita Líquida por Unidade',
                    labels={'x': 'Receita (R$)', 'y': 'Unidade'},
                    color=unidades_receita.values,
                    color_continuous_scale='Blues'
                )
                fig_unidades.update_layout(showlegend=False)
                st.plotly_chart(fig_unidades, use_container_width=True)
            
            # Produtos e Cupons
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🎁 Top 10 Produtos")
                
                produtos_qtd = df_omni['PACKAGE_NAME'].value_counts().head(10)
                
                fig_produtos = px.bar(
                    x=produtos_qtd.values,
                    y=produtos_qtd.index,
                    orientation='h',
                    title='Produtos Mais Vendidos',
                    labels={'x': 'Quantidade', 'y': 'Produto'},
                    color=produtos_qtd.values,
                    color_continuous_scale='Greens'
                )
                fig_produtos.update_layout(showlegend=False)
                st.plotly_chart(fig_produtos, use_container_width=True)
            
            with col2:
                st.subheader("🎟️ Top 10 Cupons")
                
                df_com_cupom = df_omni[df_omni['COUPONS'].notna() & (df_omni['COUPONS'] != '')]
                
                if not df_com_cupom.empty:
                    cupons_qtd = df_com_cupom['COUPONS'].value_counts().head(10)
                    
                    fig_cupons = px.bar(
                        x=cupons_qtd.values,
                        y=cupons_qtd.index,
                        orientation='h',
                        title='Cupons Mais Utilizados',
                        labels={'x': 'Quantidade', 'y': 'Cupom'},
                        color=cupons_qtd.values,
                        color_continuous_scale='Oranges'
                    )
                    fig_cupons.update_layout(showlegend=False)
                    st.plotly_chart(fig_cupons, use_container_width=True)
                else:
                    st.info("Nenhum cupom utilizado no período")
        
        else:
            st.warning("Sem dados de vouchers omnichannel para o período selecionado")
    
    except Exception as e:
        st.error(f"Erro ao carregar vouchers omnichannel: {str(e)}")

# ========================================
# ABA: SELF-SERVICE
# ========================================

with tab5:
    st.header("🤖 Self-Service")
    st.info("🚧 Seção em desenvolvimento")

# ========================================
# ABA: SUPORTE
# ========================================

with tab6:
    st.header("📞 Suporte & Chamados")
    
    st.info("🚧 Seção em desenvolvimento. Dados de chamados serão exibidos em breve.")
    
    st.subheader("📊 Visão Geral de Chamados")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total de Chamados", "-")
    
    with col2:
        st.metric("Chamados Abertos", "-")
    
    with col3:
        st.metric("Taxa de Resolução", "-")
    
    with col4:
        st.metric("Tempo Médio", "-")
    
    st.subheader("📈 Evolução de Chamados")
    st.info("Gráfico de linha temporal será exibido aqui")
    
    st.subheader("🏢 Chamados por Departamento")
    st.info("Gráfico de barras será exibido aqui")

# ========================================
# ABA: GLOSSÁRIO
# ========================================

with tab7:
    st.header("📖 Glossário")
    
    st.subheader("🎫 Vouchers & Ecommerce")
    
    with st.expander("Status de Vouchers"):
        st.write("""
        - **Status 1 - Disponível**: Voucher foi vendido mas ainda não foi utilizado
        - **Status 2 - Utilizado**: Voucher foi usado pelo cliente
        - **Status 3 - Utilizado**: Voucher foi usado pelo cliente (status alternativo)
        """)
    
    with st.expander("Tipos de Receita"):
        st.write("""
        - **Receita Bruta (PRICE_GROSS)**: Valor total antes de descontos
        - **Receita Líquida (PRICE_NET)**: Valor final após descontos e cupons
        - **Desconto**: Diferença entre receita bruta e líquida
        """)
    
    with st.expander("Datas Importantes"):
        st.write("""
        - **CREATED_DATE**: Data em que o voucher foi criado/vendido
        - **USED_DATE**: Data em que o voucher foi utilizado
        - **Vouchers Omnichannel**: Filtrados por CREATED_DATE (data de venda)
        - **Vouchers Utilizados**: Filtrados por USED_DATE (data de uso)
        """)
    
    with st.expander("Cupons de Desconto"):
        st.write("""
        - **Vouchers com Cupom**: Quantidade de vendas que utilizaram cupom
        - **Receita com Cupons**: Valor líquido das vendas com cupom aplicado
        - **Desconto Total Aplicado**: Diferença entre preço bruto e líquido nas vendas com cupom
        """)

# ========================================
# RODAPÉ
# ========================================

st.divider()
st.caption("Buddha Spa Dashboard v2.0 | Última atualização: " + datetime.now().strftime("%d/%m/%Y %H:%M"))
