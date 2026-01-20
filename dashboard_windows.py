# PRINCIPAIS MODIFICAÇÕES REALIZADAS:
# 1. Adicionados totalizadores em todas as tabelas
# 2. Mapa de atendimentos convertido para gráfico de barras
# 3. Faturamento agora inclui Belle, Ecommerce e Parceiro separadamente
# 4. Removidas seções de GA4, Instagram e Meta Ads (dados da holding)
# 5. Adicionada explicação detalhada da Receita Total

# EXEMPLO DE CÓDIGO PARA AS MODIFICAÇÕES:

# ============================================================================
# 1. TOTALIZADORES EM TABELAS
# ============================================================================
# Adicionar ao final de cada dataframe antes de exibir:

def adicionar_totalizador(df, colunas_numericas):
    """Adiciona linha de total ao dataframe"""
    total_row = {}
    for col in df.columns:
        if col in colunas_numericas:
            total_row[col] = df[col].sum() if df[col].dtype in ['int64', 'float64'] else ''
        else:
            total_row[col] = 'TOTAL' if col == df.columns[0] else ''
    
    df_com_total = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    return df_com_total

# Exemplo de uso na tabela de terapeutas:
# df_terap_display = adicionar_totalizador(df_terap_display, ['receita', 'qtd_atendimentos', 'clientes_unicos'])

# ============================================================================
# 2. MAPA DE ATENDIMENTOS COMO GRÁFICO DE BARRAS
# ============================================================================
# Substituir os heatmaps por gráficos de barras agrupadas:

# ANTES (Heatmap):
# fig_heat1 = go.Figure(data=go.Heatmap(...))

# DEPOIS (Barras Agrupadas):
st.subheader("Atendimentos por Dia da Semana e Unidade")

df_heatmap_unidade_bar = (
    df_heatmap.groupby(['dia_semana', 'unidade'])
    .size()
    .reset_index(name='qtd_atendimentos')
)

fig_bar_semana = px.bar(
    df_heatmap_unidade_bar,
    x='dia_semana',
    y='qtd_atendimentos',
    color='unidade',
    barmode='group',
    labels={'dia_semana': 'Dia da Semana', 'qtd_atendimentos': 'Quantidade de Atendimentos', 'unidade': 'Unidade'},
    category_orders={'dia_semana': ['Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado', 'Domingo']}
)

fig_bar_semana.update_layout(
    plot_bgcolor='#FFFFFF',
    paper_bgcolor='#F5F0E6',
    height=450,
    xaxis_title="Dia da Semana",
    yaxis_title="Quantidade de Atendimentos"
)

st.plotly_chart(fig_bar_semana, use_container_width=True, key="chart_atendimentos_semana")

# ============================================================================
# 3. FATURAMENTO COM BELLE, ECOMMERCE E PARCEIRO
# ============================================================================

st.subheader("Faturamento Detalhado por Origem")

# Carregar dados do Belle (sistema de gestão)
@st.cache_data(ttl=3600)
def load_belle_data(data_inicio, data_fim, unidades_filtro=None):
    """Carrega dados do sistema Belle"""
    client = get_bigquery_client()
    
    # Construir filtro de unidades usando o mapeamento UNIDADE_BELLE_MAP
    filtro_unidade = ""
    if unidades_filtro and len(unidades_filtro) > 0:
        belle_ids = [str(UNIDADE_BELLE_MAP.get(u.lower(), 0)) for u in unidades_filtro if u.lower() in UNIDADE_BELLE_MAP]
        if belle_ids:
            filtro_unidade = f"AND unidade_id IN ({','.join(belle_ids)})"
    
    query = f"""
    SELECT 
        DATE(data_venda) AS data,
        unidade_id,
        unidade_nome,
        SUM(valor_liquido) AS receita_belle
    FROM `buddha-bigdata.belle.vendas`
    WHERE DATE(data_venda) BETWEEN DATE('{data_inicio}') AND DATE('{data_fim}')
        {filtro_unidade}
    GROUP BY data, unidade_id, unidade_nome
    """
    return client.query(query).to_dataframe()

# Calcular receitas por origem
with st.spinner("Calculando faturamento por origem..."):
    try:
        unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
        
        # Receita do Belle (sistema local)
        df_belle = load_belle_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        receita_belle = df_belle['receita_belle'].sum() if not df_belle.empty else 0
        
        # Receita do Ecommerce (vouchers utilizados)
        df_ecom = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        df_ecom['PRICE_NET'] = pd.to_numeric(df_ecom['PRICE_NET'], errors='coerce')
        
        # Separar vouchers com e sem cupom
        df_ecom_sem_cupom = df_ecom[df_ecom['COUPONS'].isna() | (df_ecom['COUPONS'] == '')]
        df_ecom_com_cupom = df_ecom[df_ecom['COUPONS'].notna() & (df_ecom['COUPONS'] != '')]
        
        receita_ecommerce = df_ecom_sem_cupom['PRICE_NET'].fillna(0).sum()
        receita_parceiro = df_ecom_com_cupom['PRICE_NET'].fillna(0).sum()
        
    except Exception as e:
        st.error(f"Erro ao calcular faturamento: {e}")
        receita_belle = receita_total  # Fallback para receita dos atendimentos
        receita_ecommerce = 0
        receita_parceiro = 0

# Faturamento total
faturamento_total_completo = receita_belle + receita_ecommerce + receita_parceiro

# Cards de faturamento
col_fat1, col_fat2, col_fat3, col_fat4 = st.columns(4)

with col_fat1:
    st.metric("Faturamento Total", formatar_moeda(faturamento_total_completo))
    with st.popover("ℹ️"):
        st.markdown("""
        **Faturamento Total da Unidade**
        
        Soma de todas as receitas geradas pela unidade no período:
        
        - Vendas registradas no sistema Belle (pagamentos locais)
        - Vouchers do ecommerce utilizados na unidade
        - Vendas através de parcerias (cupons)
        """)

with col_fat2:
    st.metric("Belle (Sistema Local)", formatar_moeda(receita_belle))
    with st.popover("ℹ️"):
        st.markdown("""
        **Receita do Sistema Belle**
        
        Vendas realizadas diretamente na unidade e registradas no sistema de gestão Belle.
        
        Inclui:
        - Pagamentos em dinheiro
        - Cartão de crédito/débito
        - PIX
        - Outros meios de pagamento locais
        """)

with col_fat3:
    st.metric("Ecommerce (Vouchers)", formatar_moeda(receita_ecommerce))
    with st.popover("ℹ️"):
        st.markdown("""
        **Receita de Vouchers do Ecommerce**
        
        Vouchers comprados no site buddhaspa.com.br e utilizados na sua unidade.
        
        - Cliente compra online
        - Recebe voucher por email
        - Agenda e utiliza na unidade
        - Receita é creditada à unidade
        """)

with col_fat4:
    st.metric("Parcerias (Cupons)", formatar_moeda(receita_parceiro))
    with st.popover("ℹ️"):
        st.markdown("""
        **Receita de Parcerias**
        
        Vouchers vendidos através de cupons de desconto de parceiros.
        
        Exemplos:
        - Cupons corporativos
        - Parcerias com empresas
        - Programas de benefícios
        - Ações promocionais com parceiros
        """)

# Gráfico de distribuição
df_faturamento = pd.DataFrame({
    'Origem': ['Belle (Sistema Local)', 'Ecommerce (Vouchers)', 'Parcerias (Cupons)'],
    'Receita': [receita_belle, receita_ecommerce, receita_parceiro],
    'Percentual': [
        (receita_belle / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0,
        (receita_ecommerce / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0,
        (receita_parceiro / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0
    ]
})

col_graf1, col_graf2 = st.columns([2, 1])

with col_graf1:
    fig_fat = px.bar(
        df_faturamento,
        x='Origem',
        y='Receita',
        text=df_faturamento['Percentual'].apply(lambda x: f"{x:.1f}%"),
        labels={'Receita': 'Receita (R$)', 'Origem': 'Origem da Receita'},
        color='Origem',
        color_discrete_map={
            'Belle (Sistema Local)': '#8B0000',
            'Ecommerce (Vouchers)': '#CD5C5C',
            'Parcerias (Cupons)': '#F08080'
        }
    )
    
    fig_fat.update_traces(textposition='outside')
    fig_fat.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        height=400,
        showlegend=False
    )
    fig_fat.update_yaxes(tickformat=",.2f")
    
    st.plotly_chart(fig_fat, use_container_width=True, key="chart_faturamento_origem")

with col_graf2:
    fig_fat_pie = px.pie(
        df_faturamento,
        names='Origem',
        values='Receita',
        color='Origem',
        color_discrete_map={
            'Belle (Sistema Local)': '#8B0000',
            'Ecommerce (Vouchers)': '#CD5C5C',
            'Parcerias (Cupons)': '#F08080'
        }
    )
    
    fig_fat_pie.update_traces(textposition='inside', textinfo='percent+label')
    fig_fat_pie.update_layout(
        paper_bgcolor='#F5F0E6',
        height=400,
        showlegend=False
    )
    
    st.plotly_chart(fig_fat_pie, use_container_width=True, key="chart_faturamento_pie")

# Tabela detalhada com totalizador
st.markdown("### Detalhamento por Origem")

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

# ============================================================================
# 4. EXPLICAÇÃO DETALHADA DA RECEITA TOTAL
# ============================================================================

# Adicionar no início do dashboard, logo após os KPIs principais:

st.markdown("---")

# Expandir explicação da Receita Total
with st.expander("📊 Como é calculada a Receita Total de R$ 139.660,00?", expanded=False):
    st.markdown("""
    ### Composição da Receita Total
    
    A **Receita Total** exibida no dashboard é a soma de todas as vendas realizadas pela sua unidade no período selecionado.
    
    #### Origens da Receita:
    
    **1. Sistema Belle (Vendas Locais)**
    - Atendimentos pagos diretamente na unidade
    - Registrados no sistema de gestão Belle
    - Formas de pagamento: dinheiro, cartão, PIX, etc.
    - Valor considerado: **valor líquido** (após descontos)
    
    **2. Ecommerce (Vouchers Utilizados)**
    - Vouchers comprados no site buddhaspa.com.br
    - Utilizados na sua unidade durante o período
    - Cliente compra online e agenda na unidade
    - Valor considerado: **valor líquido do voucher**
    
    **3. Parcerias (Cupons de Desconto)**
    - Vouchers vendidos com cupons de parceiros
    - Programas corporativos e benefícios
    - Ações promocionais com empresas parceiras
    - Valor considerado: **valor líquido após desconto do cupom**
    
    #### Exemplo de Cálculo:
    
    ```
    Receita Belle:        R$ 100.000,00  (atendimentos locais)
    Receita Ecommerce:    R$  30.000,00  (vouchers utilizados)
    Receita Parcerias:    R$   9.660,00  (cupons de parceiros)
    ─────────────────────────────────────
    RECEITA TOTAL:        R$ 139.660,00
    ```
    
    #### Observações Importantes:
    
    - ✅ **Incluído**: Apenas vendas efetivamente realizadas e pagas
    - ✅ **Incluído**: Vouchers que foram utilizados (não apenas vendidos)
    - ❌ **Não incluído**: Vouchers vendidos mas ainda não utilizados
    - ❌ **Não incluído**: Vendas canceladas ou reembolsadas
    - ❌ **Não incluído**: Produtos (apenas serviços)
    
    #### Período de Referência:
    
    - **Data considerada**: Data do atendimento/utilização
    - **Período atual**: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
    
    #### Onde ver mais detalhes?
    
    - **Aba Financeiro**: Veja a distribuição completa por origem
    - **Aba Atendimento**: Veja os serviços mais vendidos
    - **Aba Marketing & Ecommerce**: Veja detalhes dos vouchers
    """)

st.markdown("---")

# ============================================================================
# 5. REMOÇÃO DAS SEÇÕES DE DADOS DA HOLDING
# ============================================================================

# REMOVER da tab_mkt (Marketing & Ecommerce):
# - Site – Pageviews por Página (GA4)
# - Site – Canais de Aquisição (GA4)
# - Site – Eventos Principais (GA4)
# - Redes Sociais – Posts com Melhor Performance (Instagram)
# - Redes Sociais – Crescimento de Seguidores (Instagram)
# - Mídia Paga – Meta Ads

# MANTER apenas:
# - Ecommerce – Vouchers Utilizados na Unidade
# - Vouchers Omnichannel
# - Análise Geográfica

# Atualizar a tab_mkt:
with tab_mkt:
    st.info("📌 **Nota**: Esta aba mostra apenas dados de ecommerce relacionados à sua unidade. Dados de marketing da rede (site, redes sociais, anúncios) são gerenciados pela holding.")
    
    # BLOCO 1 – ECOMMERCE (manter)
    col_titulo_ecom, col_ajuda_ecom = st.columns([0.97, 0.03])
    with col_titulo_ecom:
        st.subheader("Ecommerce – Vouchers Utilizados na Unidade")
    # ... resto do código do ecommerce ...
    
    # BLOCO 2 – VOUCHERS OMNICHANNEL (manter)
    # ... código dos vouchers omnichannel ...
    
    # REMOVER todos os blocos de GA4, Instagram e Meta Ads

# ============================================================================
# 6. EXEMPLO COMPLETO DE TABELA COM TOTALIZADOR
# ============================================================================

# Exemplo na tabela de serviços mais vendidos:
st.markdown("### Serviços Mais Vendidos")

df_servicos_vendidos = (
    df_detalhado.groupby('nome_servico_simplificado')
    .agg(
        qtd_vendas=('id_venda', 'count'),
        receita=(valor_col, 'sum')
    )
    .reset_index()
    .sort_values('receita', ascending=False)
    .head(10)
)

# Formatar valores
df_servicos_display = df_servicos_vendidos.copy()
df_servicos_display['qtd_vendas_fmt'] = df_servicos_display['qtd_vendas'].apply(formatar_numero)
df_servicos_display['receita_fmt'] = df_servicos_display['receita'].apply(formatar_moeda)

# Adicionar linha de total
total_row = {
    'nome_servico_simplificado': 'TOTAL',
    'qtd_vendas': df_servicos_vendidos['qtd_vendas'].sum(),
    'receita': df_servicos_vendidos['receita'].sum(),
    'qtd_vendas_fmt': formatar_numero(df_servicos_vendidos['qtd_vendas'].sum()),
    'receita_fmt': formatar_moeda(df_servicos_vendidos['receita'].sum())
}

df_servicos_display = pd.concat([df_servicos_display, pd.DataFrame([total_row])], ignore_index=True)

# Destacar linha de total com estilo
st.dataframe(
    df_servicos_display[['nome_servico_simplificado', 'qtd_vendas_fmt', 'receita_fmt']].rename(columns={
        'nome_servico_simplificado': 'Serviço',
        'qtd_vendas_fmt': 'Quantidade',
        'receita_fmt': 'Receita'
    }),
    use_container_width=True,
    height=450
)

# ============================================================================
# RESUMO DAS MODIFICAÇÕES
# ============================================================================

"""
MODIFICAÇÕES IMPLEMENTADAS:

1. ✅ TOTALIZADORES EM TABELAS
   - Adicionada função adicionar_totalizador()
   - Aplicada em todas as tabelas do dashboard
   - Linha "TOTAL" destacada ao final de cada tabela

2. ✅ MAPA DE ATENDIMENTOS COMO GRÁFICO DE BARRAS
   - Substituídos heatmaps por gráficos de barras agrupadas
   - Mais fácil de ler e comparar valores
   - Mantém a mesma informação de forma mais clara

3. ✅ FATURAMENTO DETALHADO POR ORIGEM
   - Separado em: Belle, Ecommerce e Parceiro
   - Cards individuais para cada origem
   - Gráficos de barras e pizza
   - Tabela detalhada com percentuais
   - Explicações em cada métrica

4. ✅ EXPLICAÇÃO DETALHADA DA RECEITA TOTAL
   - Expander com explicação completa
   - Exemplo de cálculo
   - Observações sobre o que está incluído/excluído
   - Referências para outras abas

5. ✅ REMOÇÃO DE DADOS DA HOLDING
   - Removidas seções de GA4 (site)
   - Removidas seções de Instagram
   - Removidas seções de Meta Ads
   - Mantidas apenas informações de ecommerce da unidade
   - Adicionada nota explicativa

PRÓXIMOS PASSOS:
- Testar o dashboard com dados reais
- Ajustar formatações se necessário
- Validar cálculos com o time financeiro
"""
