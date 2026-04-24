import pandas as pd
import h3
import json
import os
import multiprocessing
from datetime import timedelta
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

# --- CONFIGURAÇÕES ---
PASTA_PROJETO = '.'
ARQUIVO_CSV = os.path.join(PASTA_PROJETO, 'sinistros_municipais_2020_a_2025.csv')
PASTA_SAIDA = os.path.join(PASTA_PROJETO, 'dados_diarios')
ARQUIVO_ESTATISTICAS = os.path.join(PASTA_PROJETO, 'estatisticas_periodo.js')
RESOLUCAO_H3 = 9

if not os.path.exists(PASTA_SAIDA): 
    os.makedirs(PASTA_SAIDA)

def calcular_custo(row):
    try:
        if str(row['flg_obito']).lower() in ['verdadeiro', 'true', '1']: return 592941.73
        if str(row['flg_ferimento_leve']).lower() in ['verdadeiro', 'true', '1'] or \
           str(row['flg_ferimento_grave']).lower() in ['verdadeiro', 'true', '1']: return 71655.30
        return 13590.86
    except: return 13590.86

def processar_unico_dia(data_ref, df_full, col_veiculo, pasta_saida):
    dt_str = data_ref.strftime('%Y-%m-%d')
    caminho_arquivo = os.path.join(pasta_saida, f"{dt_str}.js")
    
    m6a, m6d = data_ref - timedelta(days=180), data_ref + timedelta(days=180)
    m3a, m3d = data_ref - timedelta(days=90), data_ref + timedelta(days=90)
    
    mask_periodo = (df_full['data_sinistro'] >= m6a) & (df_full['data_sinistro'] <= m6d)
    df_p = df_full[mask_periodo].copy()
    
    df_dia_exato = df_full[df_full['data_sinistro'] == data_ref]
    custo_total_dia = float(df_dia_exato['custo'].sum())
    
    if df_p.empty:
        with open(caminho_arquivo, 'w') as f: 
            f.write(f"var dadosDia = {{'metadata': {{'custo_dia': 0, 'diff_global': 0}}, 'data': {{}}}}")
        return dt_str, 0, 0.0

    c6a_global = df_p[df_p['data_sinistro'] < data_ref]['custo'].sum()
    c6d_global = df_p[df_p['data_sinistro'] >= data_ref]['custo'].sum()
    diff_global = float(c6d_global - c6a_global)

    json_dia = {"metadata": { "custo_dia": custo_total_dia, "diff_global": diff_global }, "data": {}}
    
    verdes = 0
    for idx, df_h3 in df_p.groupby('h3_index'):
        c6a = float(df_h3[(df_h3['data_sinistro'] >= m6a) & (df_h3['data_sinistro'] < data_ref)]['custo'].sum())
        c6d = float(df_h3[(df_h3['data_sinistro'] >= data_ref) & (df_h3['data_sinistro'] <= m6d)]['custo'].sum())
        if (c6d - c6a) < -1000: verdes += 1
        json_dia["data"][idx] = {
            "m": { "6a": c6a, "3a": float(df_h3[(df_h3['data_sinistro'] >= m3a) & (df_h3['data_sinistro'] < data_ref)]['custo'].sum()),
                   "3d": float(df_h3[(df_h3['data_sinistro'] >= data_ref) & (df_h3['data_sinistro'] < m3d)]['custo'].sum()), "6d": c6d },
            "va": df_h3[(df_h3['data_sinistro'] >= m3a) & (df_h3['data_sinistro'] < data_ref)][col_veiculo].value_counts().to_dict(),
            "vd": df_h3[(df_h3['data_sinistro'] >= data_ref) & (df_h3['data_sinistro'] < m3d)][col_veiculo].value_counts().to_dict()
        }
    with open(caminho_arquivo, 'w', encoding='utf-8') as f:
        f.write(f"var dadosDia = {json.dumps(json_dia)};")
    return dt_str, verdes, custo_total_dia

if __name__ == '__main__':
    print("1/4 - Carregando e Limpando dados...")
    df = pd.read_csv(ARQUIVO_CSV)
    
    # Converte coordenadas para numérico e remove NaNs
    df['num_latitude'] = pd.to_numeric(df['num_latitude'], errors='coerce')
    df['num_longitude'] = pd.to_numeric(df['num_longitude'], errors='coerce')
    df = df.dropna(subset=['num_latitude', 'num_longitude'])
    
    # REMOVE COORDENADAS INVÁLIDAS (Limites do H3 e valores zerados)
    df = df[(df['num_latitude'] != 0) & (df['num_longitude'] != 0)]
    df = df[(df['num_latitude'].between(-90, 90)) & (df['num_longitude'].between(-180, 180))]
    
    df['data_sinistro'] = pd.to_datetime(df['data_sinistro'])
    df['custo'] = df.apply(calcular_custo, axis=1)

    print("2/4 - Gerando Índices H3...")
    # Criar h3_index de forma segura
    df['h3_index'] = df.apply(lambda x: h3.latlng_to_cell(x['num_latitude'], x['num_longitude'], RESOLUCAO_H3), axis=1)

    geometria = {idx: h3.cell_to_boundary(idx) for idx in df['h3_index'].unique()}
    with open(os.path.join(PASTA_PROJETO, 'geometria_h3.js'), 'w', encoding='utf-8') as f:
        f.write(f"var geometriaH3 = {json.dumps(geometria)};")

    datas_alvo = pd.date_range(start='2020-01-01', end='2025-03-01')
    col_veiculo = next((c for c in df.columns if 'veiculo' in c.lower()), 'des_tipo_veiculo')
    
    num_cores = multiprocessing.cpu_count()
    estatisticas = {"minVerdes": 0, "maxVerdes": 0, "historico": {}, "custos_diarios": {}, "dataMax": "", "dataMin": ""}

    print(f"3/4 - Processando dias com {num_cores} núcleos...")
    with ProcessPoolExecutor(max_workers=num_cores) as executor:
        futures = {executor.submit(processar_unico_dia, d, df, col_veiculo, PASTA_SAIDA): d for d in datas_alvo}
        for future in tqdm(futures, total=len(futures)):
            dt_str, verdes, custo_dia = future.result()
            estatisticas["historico"][dt_str] = verdes
            estatisticas["custos_diarios"][dt_str] = custo_dia

    with open(ARQUIVO_ESTATISTICAS, 'w', encoding='utf-8') as f:
        f.write(f"var estatisticasPeriodo = {json.dumps(estatisticas)};")
    print("Processamento concluído com sucesso!")