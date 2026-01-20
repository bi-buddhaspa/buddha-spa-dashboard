"""
INSTRUÇÕES PARA APLICAR AS MODIFICAÇÕES NO SEU DASHBOARD

Copie e cole cada seção no local indicado do seu código original.
"""

# ==============================================================================
# MODIFICAÇÃO 1: ADICIONAR FUNÇÃO DE TOTALIZADOR (logo após as funções de formatação)
# ==============================================================================
# LOCALIZAÇÃO: Adicionar após a função formatar_percentual(), linha ~50

def adicionar_totalizador(df, colunas_numericas, primeira_coluna=''):
    """
    Adiciona linha de total ao dataframe
    
    Args:
        df: DataFrame original
        colunas_numericas: lista de colunas que devem ser somadas
        primeira_coluna: nome da primeira coluna (onde aparecerá 'TOTAL')
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
                # Se já está formatado como string, tentar extrair número
                total_row[col] = ''
        else:
            # Primeira coluna recebe 'TOTAL'
            if col == (primeira_coluna or df.columns[0]):
                total_row[col] = 'TOTAL'
            else:
                total_row[col] = ''
    
    df_com_total = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    return df_com_total


# ==============================================================================
# MODIFICAÇÃO 2: EXPLICAÇÃO DA RECEITA TOTAL
# ==============================================================================
# LOCALIZAÇÃO: Adicionar logo após os KPIs principais (após st.divider(), linha ~850)

# Adicionar ANTES de st.divider():
st.markdown("---")

# Expandir explicação da Receita Total
with st.expander("📊 De onde vem a Receita Total?", expanded=False):
    st.markdown(f"""
    ### Como calculamos os **{formatar_moeda(receita_total)}**?
    
    A Receita Total é composta por todas as vendas de **serviços** realizadas na sua unidade durante o período selecionado.
    
    #### 📍 O que está incluído:
    
    **Atendimentos Presenciais Pagos**
    - Todos os serviços realizados e pagos na unidade
    - Formas de pagamento: dinheiro, cartão, PIX, etc.
    - Apenas o **valor líquido** (já descontado impostos e taxas)
    
    #### 🔍 Detalhamento:
    
    - **Período**: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}
    - **Total de atendimentos**: {formatar_numero(qtd_atendimentos)}
    - **Clientes únicos**: {formatar_numero(qtd_clientes)}
    - **Ticket médio**: {formatar_moeda(ticket_medio)}
    
    #### ✅ Incluído na receita:
    - Massagens e terapias
    - Tratamentos faciais e corporais
    - Pacotes de serviços
    - Day spa
    
    #### ❌ NÃO incluído:
    - Produtos vendidos (cosméticos, óleos, etc.)
    - Vouchers vendidos mas ainda não utilizados
    - Vendas canceladas ou reembolsadas
    
    #### 💡 Quer ver mais detalhes?
    
    - **Aba Financeiro**: Veja a distribuição completa por origem (Belle, Ecommerce, Parcerias)
    - **Aba Atendimento**: Veja quais serviços geraram mais receita
    - **Aba Marketing & Ecommerce**: Veja os vouchers utilizados
    """)


# ==============================================================================
# MODIFICAÇÃO 3: SUBSTITUIR HEATMAP 1 POR GRÁFICO DE BARRAS
# ==============================================================================
# LOCALIZAÇÃO: Substituir o código do HEATMAP 1 (linha ~1450 aproximadamente)
# PROCURAR POR: "# HEATMAP 1: Atendimentos por Dia da Semana vs Unidade"

# SUBSTITUIR TODO O BLOCO DO HEATMAP 1 POR:

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
        category_orders={'dia_semana': dias_ordem}
    )
    
    fig_bar1.update_layout(
        plot_bgcolor='#FFFFFF',
        paper_bgcolor='#F5F0E6',
        height=450,
        xaxis_title="Dia da Semana",
        yaxis_title="Quantidade de Atendimentos",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig_bar1, use_container_width=True, key="chart_bar_semana_unidade")


# ==============================================================================
# MODIFICAÇÃO 4: SUBSTITUIR HEATMAP 2 POR GRÁFICO DE BARRAS
# ==============================================================================
# LOCALIZAÇÃO: Substituir o código do HEATMAP 2 (logo após o HEATMAP 1)
# PROCURAR POR: "# HEATMAP 2: Atendimentos por Dia da Semana vs Tipo de Serviço"

# SUBSTITUIR TODO O BLOCO DO HEATMAP 2 POR:

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
            category_orders={'dia_semana': dias_ordem}
        )
        
        fig_bar2.update_layout(
            plot_bgcolor='#FFFFFF',
            paper_bgcolor='#F5F0E6',
            height=450,
            xaxis_title="Dia da Semana",
            yaxis_title="Quantidade de Atendimentos",
            legend=dict(orientation="v", yanchor="top", y=1, xanchor="left", x=1.02, title="Serviço")
        )
        
        st.plotly_chart(fig_bar2, use_container_width=True, key="chart_bar_semana_servico")


# ==============================================================================
# MODIFICAÇÃO 5: ADICIONAR TOTALIZADOR NA TABELA DE TERAPEUTAS
# ==============================================================================
# LOCALIZAÇÃO: Na aba Atendimento, na seção "Tabela de Performance"
# PROCURAR POR: st.markdown("### Tabela de Performance")

# SUBSTITUIR o bloco de formatação e exibição da tabela por:

        st.markdown("### Tabela de Performance")
        
        # Formatar valores
        df_terap_display = df_terap.copy()
        
        # Adicionar totalizador ANTES de formatar
        df_terap_com_total = adicionar_totalizador(
            df_terap_display, 
            colunas_numericas=['receita', 'qtd_atendimentos', 'clientes_unicos', 'ticket_medio'],
            primeira_coluna='unidade'
        )
        
        # Agora formatar
        df_terap_com_total['receita'] = df_terap_com_total['receita'].apply(
            lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
        )
        df_terap_com_total['qtd_atendimentos'] = df_terap_com_total['qtd_atendimentos'].apply(
            lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
        )
        df_terap_com_total['clientes_unicos'] = df_terap_com_total['clientes_unicos'].apply(
            lambda x: formatar_numero(x) if pd.notna(x) and x != '' else x
        )
        df_terap_com_total['ticket_medio'] = df_terap_com_total['ticket_medio'].apply(
            lambda x: formatar_moeda(x) if pd.notna(x) and x != '' else x
        )
        
        st.dataframe(
            df_terap_com_total,
            use_container_width=True,
            height=500
        )


# ==============================================================================
# MODIFICAÇÃO 6: FATURAMENTO DETALHADO (BELLE + ECOMMERCE + PARCEIRO)
# ==============================================================================
# LOCALIZAÇÃO: Na aba Financeiro, SUBSTITUIR a seção "Distribuição de Receita por Canal"
# PROCURAR POR: col_titulo_dist, col_ajuda_dist = st.columns([0.97, 0.03])

# SUBSTITUIR TODO O BLOCO até st.markdown("---") por:

    # Faturamento Detalhado por Origem
    col_titulo_fat, col_ajuda_fat = st.columns([0.97, 0.03])
    with col_titulo_fat:
        st.subheader("Faturamento Detalhado por Origem")
    with col_ajuda_fat:
        with st.popover("ℹ️"):
            st.markdown("""
            **Origens do Faturamento:**
            
            - **Belle (Sistema Local)**: Vendas registradas no sistema de gestão da unidade
            - **Ecommerce (Vouchers)**: Vouchers comprados online e utilizados na unidade
            - **Parcerias (Cupons)**: Vendas através de cupons de parceiros
            """)
    
    with st.spinner("Calculando faturamento por origem..."):
        try:
            unidades_para_filtro = unidades_selecionadas if is_admin else [unidade_usuario.lower()]
            df_ecom_fat = load_ecommerce_data(data_inicio, data_fim, unidades_filtro=unidades_para_filtro)
        except Exception as e:
            st.error(f"Erro ao carregar ecommerce: {e}")
            df_ecom_fat = pd.DataFrame()
    
    # Calcular receitas por origem
    receita_belle = receita_total  # Receita dos atendimentos presenciais
    receita_ecommerce = 0
    receita_parceiro = 0
    
    if not df_ecom_fat.empty:
        df_ecom_fat['PRICE_NET'] = pd.to_numeric(df_ecom_fat['PRICE_NET'], errors='coerce')
        
        # Separar vouchers com e sem cupom
        df_ecom_sem_cupom = df_ecom_fat[df_ecom_fat['COUPONS'].isna() | (df_ecom_fat['COUPONS'] == '')]
        df_ecom_com_cupom = df_ecom_fat[df_ecom_fat['COUPONS'].notna() & (df_ecom_fat['COUPONS'] != '')]
        
        receita_ecommerce = df_ecom_sem_cupom['PRICE_NET'].fillna(0).sum()
        receita_parceiro = df_ecom_com_cupom['PRICE_NET'].fillna(0).sum()
    
    faturamento_total_completo = receita_belle + receita_ecommerce + receita_parceiro
    
    # Cards de faturamento
    col_fat1, col_fat2, col_fat3, col_fat4 = st.columns(4)
    
    with col_fat1:
        st.metric("💰 Faturamento Total", formatar_moeda(faturamento_total_completo))
        with st.popover("ℹ️"):
            st.caption("Soma de todas as receitas: Belle + Ecommerce + Parcerias")
    
    with col_fat2:
        st.metric("🏪 Belle (Sistema Local)", formatar_moeda(receita_belle))
        with st.popover("ℹ️"):
            st.caption("Atendimentos pagos diretamente na unidade (dinheiro, cartão, PIX)")
    
    with col_fat3:
        st.metric("🛒 Ecommerce (Vouchers)", formatar_moeda(receita_ecommerce))
        with st.popover("ℹ️"):
            st.caption("Vouchers comprados no site e utilizados na unidade")
    
    with col_fat4:
        st.metric("🤝 Parcerias (Cupons)", formatar_moeda(receita_parceiro))
        with st.popover("ℹ️"):
            st.caption("Vendas através de cupons de parceiros e empresas")
    
    # Gráficos de distribuição
    df_faturamento = pd.DataFrame({
        'Origem': ['Belle\n(Sistema Local)', 'Ecommerce\n(Vouchers)', 'Parcerias\n(Cupons)'],
        'Receita': [receita_belle, receita_ecommerce, receita_parceiro],
        'Percentual': [
            (receita_belle / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0,
            (receita_ecommerce / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0,
            (receita_parceiro / faturamento_total_completo * 100) if faturamento_total_completo > 0 else 0
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
                'Ecommerce\n(Vouchers)': '#CD5C5C',
                'Parcerias\n(Cupons)': '#F08080'
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
                'Ecommerce\n(Vouchers)': '#CD5C5C',
                'Parcerias\n(Cupons)': '#F08080'
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


# ==============================================================================
# MODIFICAÇÃO 7: REMOVER SEÇÕES DE DADOS DA HOLDING
# ==============================================================================
# LOCALIZAÇÃO: Na aba Marketing & Ecommerce (tab_mkt)

# ADICIONAR no início da tab_mkt (logo após "with tab_mkt:"):

with tab_mkt:
    st.info("📌 **Nota para Franqueados**: Esta aba mostra apenas dados de ecommerce relacionados à sua unidade. Dados de marketing da rede (site, redes sociais, anúncios) são gerenciados pela holding e não aparecem aqui.")
    
    st.markdown("---")
    
    # ... resto do código de ecommerce ...


# REMOVER (ou comentar) as seguintes seções da tab_mkt:
# - BLOCO 2: Site – Pageviews por Página (GA4)
# - BLOCO 3: Site – Canais de Aquisição (GA4)
# - BLOCO 4: Site – Eventos Principais (GA4)
# - BLOCO 5: Redes Sociais – Posts Instagram
# - BLOCO 6: Redes Sociais – Seguidores Instagram
# - BLOCO 7: Mídia Paga – Meta Ads

# MANTER apenas:
# - BLOCO 1: Ecommerce – Vouchers Utilizados
# - BLOCO 1.5: Vouchers Omnichannel
# - Análise Geográfica (se houver)


# ==============================================================================
# MODIFICAÇÃO 8: ADICIONAR TOTALIZADOR EM OUTRAS TABELAS
# ==============================================================================

# Para a tabela de vouchers (na seção de ecommerce):
# PROCURAR POR: st.dataframe(df_serv_display[['KEY', 'ORDER_ID'...

# ANTES de exibir o dataframe, adicionar:
df_serv_display_total = adicionar_totalizador(
    df_serv_display,
    colunas_numericas=[],  # Não somar nada, apenas adicionar linha TOTAL
    primeira_coluna='KEY'
)

# Depois exibir:
st.dataframe(df_serv_display_total[...], ...)


# Para a tabela de serviços mais vendidos:
# Similar ao exemplo acima, adicionar totalizador antes de formatar


# ==============================================================================
# RESUMO DAS MODIFICAÇÕES
# ==============================================================================

"""
✅ CHECKLIST DE MODIFICAÇÕES:

1. ✅ Função adicionar_totalizador() criada
2. ✅ Explicação da Receita Total adicionada
3. ✅ Heatmap 1 substituído por gráfico de barras
4. ✅ Heatmap 2 substituído por gráfico de barras
5. ✅ Totalizador na tabela de terapeutas
6. ✅ Faturamento detalhado (Belle + Ecommerce + Parceiro)
7. ✅ Nota sobre dados da holding
8. ✅ Remoção de seções GA4, Instagram, Meta Ads

COMO APLICAR:
1. Abra seu arquivo dashboard_windows.py
2. Localize cada seção indicada
3. Copie e cole o código correspondente
4. Teste o dashboard
5. Ajuste se necessário
"""
